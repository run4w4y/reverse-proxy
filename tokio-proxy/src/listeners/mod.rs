#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

pub mod tcp;
// pub mod http;

#[async_trait::async_trait]
pub trait Listener {
    type Error;

    async fn io_loop(self) -> Result<(), Self::Error>;
}
