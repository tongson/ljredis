use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic;

extern crate redis;
extern crate base64;

fn cs(s: Vec<u8>) -> *const c_char {
  let c_str = CString::new(s).unwrap();
  let ptr = c_str.as_ptr();
  std::mem::forget(c_str);
  return ptr
}

#[no_mangle]
pub extern "C" fn incr(c: *const c_char) -> *const c_char {
  let nak: Vec<u8> = b"\x21".to_vec();
  let ack: Vec<u8> = b"\x06".to_vec();
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.incr()")));
  let client = match redis::Client::open("redis://127.0.0.1/") {
    Ok(client) => client,
    Err(_) => return cs(nak),
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return cs(nak),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _ : () = match redis::cmd("INCR").arg(cb).query::<Option<String>>(&mut con) {
    Ok(_) => return cs(ack),
    Err(_) => return cs(nak),
  };
}

#[no_mangle]
pub extern "C" fn get(c: *const c_char) -> *const c_char {
  let nak: Vec<u8> = b"\x21".to_vec();
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.get()")));
  let client = match redis::Client::open("redis://127.0.0.1/") {
    Ok(client) => client,
    Err(_) => return cs(nak),
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return cs(nak),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _ :() = match redis::cmd("GET").arg(cb).query::<Vec<u8>>(&mut con) {
    Ok(s) => return cs(base64::encode(s).as_bytes().to_vec()),
    Err(_) => return cs(nak),
  };
}
