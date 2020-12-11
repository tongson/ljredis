use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic;

extern crate redis;
extern crate base64;
extern crate serde_json;
use std::collections::HashMap;
use serde::{Deserialize};
use serde_json::from_slice;

const HOST: &str = "127.0.0.1";

fn cs(s: Vec<u8>) -> *const c_char {
  let c_str = CString::new(s).unwrap();
  let ptr = c_str.as_ptr();
  std::mem::forget(c_str);
  return ptr
}

#[no_mangle]
pub extern "C" fn incr(c: *const c_char) -> *const c_char {
  let ack: Vec<u8> = vec!(6);
  let dc2: Vec<u8> = vec!(18);
  let dc4: Vec<u8> = vec!(20);
  let nak: Vec<u8> = vec!(21);
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.incr()")));
  let client = match redis::Client::open(format!("redis://{}/", HOST)) {
    Ok(rc) => rc,
    Err(_) => return cs(dc2),
  };
  let mut con = match client.get_connection() {
    Ok(rc) => rc,
    Err(_) => return cs(dc4),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _ : () = match redis::cmd("INCR").arg(cb).query::<i32>(&mut con) {
    Ok(_) => return cs(ack),
    Err(_) => return cs(nak),
  };
}

#[no_mangle]
pub extern "C" fn get(c: *const c_char) -> *const c_char {
  let nak: Vec<u8> = vec!(21);
  let dc2: Vec<u8> = vec!(18);
  let dc4: Vec<u8> = vec!(20);
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.get()")));
  let client = match redis::Client::open(format!("redis://{}/", HOST)) {
    Ok(client) => client,
    Err(_) => return cs(dc2),
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return cs(dc4),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _ :() = match redis::cmd("GET").arg(cb).query::<Vec<u8>>(&mut con) {
    Ok(s) => return cs(base64::encode(s).as_bytes().to_vec()),
    Err(_) => return cs(nak),
  };
}

#[no_mangle]
pub extern "C" fn qget(c: *const c_char) -> *const c_char {
  let nak: Vec<u8> = vec!(21);
  let dc2: Vec<u8> = vec!(18);
  let dc4: Vec<u8> = vec!(20);
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.qget()")));
  let client = match redis::Client::open(format!("redis://{}/", HOST)) {
    Ok(client) => client,
    Err(_) => return cs(dc2),
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return cs(dc4),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _ :() = match redis::cmd("GET").arg(cb).query::<Vec<u8>>(&mut con) {
    Ok(s) => return cs(s),
    Err(_) => return cs(nak),
  };
}

#[no_mangle]
pub extern "C" fn set(c: *const c_char) -> *const c_char {
  let dc2: Vec<u8> = vec!(18);
  let dc4: Vec<u8> = vec!(20);
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.set()")));
  #[derive(Deserialize)]
  struct Args {
    expire: String,
    data: HashMap<String, String>,
  }
  let client = match redis::Client::open(format!("redis://{}/", HOST)) {
    Ok(client) => client,
    Err(_) => return cs(dc2),
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return cs(dc4),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let j: Args = from_slice(cb).unwrap();
  let d: HashMap<String, String> = j.data;
  let mut ret: Vec<u8> = vec!(6);
  if j.expire == "0" {
    'zloop: for (k, v) in &d {
      let _ : () = match redis::cmd("SET").arg(k).arg(v).query::<String>(&mut con) {
        Ok(_) => {},
        Err(_) => { ret = vec!(21); break 'zloop; },
      };
    }
    return cs(ret);
  } else {
    'eloop: for (k, v) in &d {
      let _ : () = match redis::cmd("SET")
        .arg(k)
        .arg(v)
        .arg("EX")
        .arg(&j.expire)
        .query::<String>(&mut con) {
          Ok(_) => {},
          Err(_) => { ret = vec!(21); break 'eloop; },
        };
    }
    return cs(ret);
  }
}
