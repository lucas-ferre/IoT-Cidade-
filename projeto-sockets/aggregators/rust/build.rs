fn main() {
    prost_build::compile_protos(&["../../common/messages.proto"], &["../../common/"]).unwrap();
}
