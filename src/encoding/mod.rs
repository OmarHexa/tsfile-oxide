// C++ encoding/ is 34 header-only files with a virtual Encoder*/Decoder*
// hierarchy and EncoderFactory/DecoderFactory for allocation. In Rust we
// use enum dispatch — the set of encodings is closed (7 algorithms), so
// a match statement replaces virtual dispatch with zero heap allocation
// and no vtable indirection.
