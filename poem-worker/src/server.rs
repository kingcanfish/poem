use async_trait::async_trait;
use poem::endpoint::Endpoint;
use tokio::sync::OnceCell;

#[worker::event(fetch)]
pub async fn fetch(
    request: worker::Request,
    _env: worker::Env,
    _ctx: worker::Context,
) -> Result<worker::Response, worker::Error> {
    let app = server_instance()?;

    let cf = request.cf().cloned();

    let http_req = worker::HttpRequest::try_from(request)?;
    let mut poem_req = crate::req::build_poem_req(http_req)?;

    if let Some(cf) = cf {
        poem_req.set_data(cf);
    }

    poem_req.set_data(crate::Env::new(_env));

    let resp = app.get_poem_response(poem_req).await;
    let worker_resp = crate::req::build_worker_resp(resp)?;
    let resp = worker::Response::try_from(worker_resp)?;

    Ok(resp)
}

/// Returns the endpoint given to [`Server::run`].
fn server_instance() -> Result<&'static BoxedGetResponseInner, worker::Error> {
    SERVER_INSTANCE.get().ok_or_else(|| {
        worker::Error::RustError(
            "no endpoint to serve the request: `poem_worker::Server::run` must be called in the \
             `start` event"
                .to_string(),
        )
    })
}

#[async_trait]
trait GetResponseInner: Send + Sync + 'static {
    async fn get_poem_response(&self, req: poem::Request) -> poem::Response;
}

#[async_trait]
impl<E: Endpoint + Send + Sync + 'static> GetResponseInner for E {
    async fn get_poem_response(&self, req: poem::Request) -> poem::Response {
        self.get_response(req).await
    }
}

pub struct Server {}

type BoxedGetResponseInner = Box<dyn GetResponseInner>;

static SERVER_INSTANCE: OnceCell<BoxedGetResponseInner> = OnceCell::const_new();

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        Self {}
    }

    pub fn run(&self, app: impl Endpoint + 'static) {
        SERVER_INSTANCE
            .set(Box::new(app))
            .map_err(|_| "Server instance can only be set once".to_string())
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The only test of this crate touching `SERVER_INSTANCE`, so that it sees
    // it before and after `Server::run`.
    #[test]
    fn server_instance_needs_run() {
        let Err(worker::Error::RustError(message)) = server_instance() else {
            panic!("expected an error before `Server::run`");
        };
        assert!(message.contains("Server::run"), "{message}");

        Server::new().run(poem::endpoint::make_sync(|_| "hello"));
        assert!(server_instance().is_ok());
    }
}
