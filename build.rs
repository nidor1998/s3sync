use shadow_rs::ShadowBuilder;

#[cfg(feature = "version")]
fn main() {
    ShadowBuilder::builder().build().unwrap();
}
#[cfg(not(feature = "version"))]
fn main() {}
