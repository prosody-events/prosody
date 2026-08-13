//! Compile-time composition of a [`Codec::FORMAT_ID`](super::Codec::FORMAT_ID).

use std::str::from_utf8;

/// Compile-time builder for a composed
/// [`Codec::FORMAT_ID`](super::Codec::FORMAT_ID) such as `"(a,b)"`.
///
/// Component ids are appended through [`Self::push`], which rejects the
/// composition delimiters `(`, `)`, `,` — a `const` panic, so it is a compile
/// error. That keeps composition **injective**: `"(a,b)"` + `c` can never
/// collide with `a` + `"b,c)"`, so two distinct compositions can never mint the
/// same durable token. Overflowing the fixed buffer is likewise a compile
/// error.
pub(super) struct ConstId {
    buf: [u8; 128],
    len: usize,
}

impl ConstId {
    pub(super) const fn new() -> Self {
        Self {
            buf: [0; 128],
            len: 0,
        }
    }

    /// Appends a component id, rejecting the reserved composition delimiters so
    /// distinct compositions stay injective.
    pub(super) const fn push(mut self, s: &str) -> Self {
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            assert!(
                bytes[i] != b'(' && bytes[i] != b')' && bytes[i] != b',',
                "codec id contains a reserved composition delimiter"
            );
            self.buf[self.len] = bytes[i]; // buffer overflow -> const panic = compile error
            self.len += 1;
            i += 1;
        }
        self
    }

    /// Appends the composition delimiters themselves, exempt from the reserved
    /// character check.
    pub(super) const fn raw(mut self, s: &str) -> Self {
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            self.buf[self.len] = bytes[i];
            self.len += 1;
            i += 1;
        }
        self
    }

    pub(super) const fn as_static_str(&'static self) -> &'static str {
        let (head, _) = self.buf.split_at(self.len);
        // Every byte was copied whole from `&str` inputs, so `head` is always
        // valid UTF-8; the assert fails closed (a compile error) if a builder
        // edit ever breaks that, rather than minting a colliding empty id.
        assert!(
            from_utf8(head).is_ok(),
            "composed codec id is not valid UTF-8"
        );
        match from_utf8(head) {
            Ok(s) => s,
            Err(_) => "",
        }
    }
}
