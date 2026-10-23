use std::time::Duration;

use durable_runtime::{ClassifyError, ErrorClass, StepError, classify::check_response};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

async fn respond(template: ResponseTemplate) -> reqwest::Response {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/x"))
        .respond_with(template)
        .mount(&server)
        .await;
    // The server is leaked on purpose: the response outlives this helper.
    let url = format!("{}/x", server.uri());
    Box::leak(Box::new(server));
    reqwest::get(url).await.unwrap()
}

#[tokio::test]
async fn success_passes_through() {
    let resp = respond(ResponseTemplate::new(200).set_body_string("ok")).await;
    let resp = check_response(resp).unwrap();
    assert_eq!(resp.text().await.unwrap(), "ok");
}

#[tokio::test]
async fn too_many_requests_honours_retry_after() {
    let resp = respond(ResponseTemplate::new(429).insert_header("Retry-After", "7")).await;
    let err = check_response(resp).unwrap_err();
    assert_eq!(err.class, ErrorClass::RetryAfter(Duration::from_secs(7)));
    assert!(err.message.contains("429"), "{}", err.message);
}

#[tokio::test]
async fn too_many_requests_without_a_usable_header_is_retryable() {
    let resp = respond(ResponseTemplate::new(429)).await;
    assert_eq!(
        check_response(resp).unwrap_err().class,
        ErrorClass::Retryable
    );
    let resp = respond(ResponseTemplate::new(429).insert_header("Retry-After", "soon")).await;
    assert_eq!(
        check_response(resp).unwrap_err().class,
        ErrorClass::Retryable
    );
}

#[tokio::test]
async fn server_errors_and_request_timeouts_are_retryable() {
    for code in [500, 502, 503, 504, 408] {
        let resp = respond(ResponseTemplate::new(code)).await;
        assert_eq!(
            check_response(resp).unwrap_err().class,
            ErrorClass::Retryable,
            "{code}"
        );
    }
}

#[tokio::test]
async fn other_client_errors_are_fatal() {
    for code in [400, 401, 403, 404, 422] {
        let resp = respond(ResponseTemplate::new(code)).await;
        assert_eq!(
            check_response(resp).unwrap_err().class,
            ErrorClass::Fatal,
            "{code}"
        );
    }
}

#[tokio::test]
async fn reqwest_errors_classify_by_status_and_kind() {
    let resp = respond(ResponseTemplate::new(404)).await;
    let err = resp.error_for_status().unwrap_err();
    assert_eq!(err.classify(), ErrorClass::Fatal);
    let resp = respond(ResponseTemplate::new(503)).await;
    assert_eq!(
        resp.error_for_status().unwrap_err().classify(),
        ErrorClass::Retryable
    );

    // Nothing listens on port 1: a connection failure is worth retrying.
    let err = reqwest::get("http://127.0.0.1:1/").await.unwrap_err();
    assert_eq!(err.classify(), ErrorClass::Retryable);

    // `StepError::classified` carries the class through.
    let step: StepError = StepError::classified(err);
    assert_eq!(step.class, ErrorClass::Retryable);
}
