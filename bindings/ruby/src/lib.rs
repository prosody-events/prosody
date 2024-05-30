use magnus::{define_global_function, function};

fn hello() -> &'static str {
    "hello Ruby"
}

#[magnus::init(name = "prosody")]
fn init() {
    define_global_function("hello", function!(hello, 0));
}
