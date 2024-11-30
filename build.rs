#[cfg(feature = "version")]
fn main() -> shadow_rs::SdResult<()> {
    shadow_rs::new()
}
#[cfg(not(feature = "version"))]
fn main() {}
