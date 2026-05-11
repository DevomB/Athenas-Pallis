#![cfg(feature = "binance-live")]

use athenas_pallas::execution::{BinanceCredentials, BinanceLiveGateway, ExecutionGateway};
use athenas_pallas::events::{OrderIntent, OrderIntentSource};
use athenas_pallas::state::{GlobalState, InstrumentMeta};
use athenas_pallas::types::{Asset, InstrumentId, OrderType, Side};
use rust_decimal::Decimal;
use std::collections::HashMap;
use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn market_order_signed_post_parses_response() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v3/order"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{
            "symbol":"BTCUSDT",
            "orderId":123456,
            "status":"FILLED",
            "executedQty":"0.01",
            "origQty":"0.01",
            "type":"MARKET",
            "side":"BUY",
            "cummulativeQuoteQty":"400.00"
        }"#,
        ))
        .mount(&server)
        .await;

    let creds = BinanceCredentials {
        api_key: "k".into(),
        secret: "s".into(),
    };
    let gw = BinanceLiveGateway::new(server.uri(), creds);
    let i = InstrumentId::new("binance", "BTCUSDT");
    let mut instruments = HashMap::new();
    instruments.insert(
        i.clone(),
        InstrumentMeta {
            base: Asset("BTC".into()),
            quote: Asset("USDT".into()),
        },
    );
    let mut balances = HashMap::new();
    balances.insert(Asset("USDT".into()), Decimal::new(10_000, 0));
    let state = GlobalState::new(instruments, balances);
    let intent = OrderIntent {
        instrument: i,
        side: Side::Buy,
        order_type: OrderType::Market,
        price: None,
        qty: Decimal::new(1, 2),
        client_order_id: None,
        source: OrderIntentSource::User,
    };
    let evs = gw.place_market(&state, &intent).await.unwrap();
    assert!(!evs.is_empty());
}
