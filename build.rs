#[cfg(feature = "version")]
fn main() {
    use shadow_rs::ShadowBuilder;
    ShadowBuilder::builder().build().unwrap();
}
#[cfg(not(feature = "version"))]
fn main() {}
