use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic;

extern crate base64;
extern crate redis;
extern crate serde_json;
use serde::Deserialize;
use serde_json::from_slice;
use std::collections::HashMap;

const ECONN: [u8; 6] = [21, 1, 67, 79, 78, 78];
const ECLIENT: [u8; 8] = [21, 1, 67, 76, 73, 69, 78, 84];
const EQUERY: [u8; 7] = [21, 1, 81, 85, 69, 82, 89];
const OK: [u8; 1] = [6];

fn pj(s: &[u8]) -> *const c_char {
  let nul: Vec<u8> = vec![21, 1, 78, 85, 76];
  let c_nul = CString::new(nul).unwrap();
  let c_str = match CString::new(s.to_vec()) {
    Ok(c_str) => c_str,
    Err(_) => c_nul,
  };
  let ptr = c_str.as_ptr();
  std::mem::forget(c_str);
  return ptr;
}

fn cs(s: Vec<u8>) -> *const c_char {
  let nul: Vec<u8> = vec![21, 1, 78, 85, 76];
  let c_nul = CString::new(nul).unwrap();
  let c_str = match CString::new(s) {
    Ok(c_str) => c_str,
    Err(_) => c_nul,
  };
  let ptr = c_str.as_ptr();
  std::mem::forget(c_str);
  return ptr;
}

#[no_mangle]
pub extern "C" fn del(h: *const c_char, c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.del()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(rc) => rc,
    Err(_) => return pj(&ECLIENT),
  };
  let mut con = match client.get_connection() {
    Ok(rc) => rc,
    Err(_) => return pj(&ECONN),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("DEL").arg(cb).query::<i32>(&mut con) {
    Ok(_) => return pj(&OK),
    Err(_) => return pj(&EQUERY),
  };
}

#[no_mangle]
pub extern "C" fn unlink(h: *const c_char, c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.unlink()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(rc) => rc,
    Err(_) => return pj(&ECLIENT),
  };
  let mut con = match client.get_connection() {
    Ok(rc) => rc,
    Err(_) => return pj(&ECONN),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("UNLINK").arg(cb).query::<i32>(&mut con) {
    Ok(_) => return pj(&OK),
    Err(_) => return pj(&EQUERY),
  };
}

#[no_mangle]
pub extern "C" fn incr(h: *const c_char, c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.incr()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(rc) => rc,
    Err(_) => return pj(&ECLIENT),
  };
  let mut con = match client.get_connection() {
    Ok(rc) => rc,
    Err(_) => return pj(&ECONN),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("INCR").arg(cb).query::<i32>(&mut con) {
    Ok(_) => return pj(&OK),
    Err(_) => return pj(&EQUERY),
  };
}

#[no_mangle]
pub extern "C" fn json_get(h: *const c_char, c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.json_get()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  #[derive(Deserialize)]
  struct Args {
    key: String,
    path: String,
  }
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(client) => client,
    Err(_) => return pj(&ECLIENT),
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return pj(&ECONN),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let j: Args = from_slice(cb).unwrap();
  let key: String = j.key;
  let path: String = j.path;
  let _: () = match redis::cmd("JSON.GET")
    .arg(key)
    .arg(path)
    .query::<Vec<u8>>(&mut con)
  {
    Ok(s) => return cs(s),
    Err(_) => return pj(&EQUERY),
  };
}

#[no_mangle]
pub extern "C" fn get(h: *const c_char, c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.get()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(client) => client,
    Err(_) => return pj(&ECLIENT),
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return pj(&ECONN),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("GET").arg(cb).query::<Vec<u8>>(&mut con) {
    Ok(s) => return cs(s),
    Err(_) => return pj(&EQUERY),
  };
}

#[no_mangle]
pub extern "C" fn json_set(h: *const c_char, c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.json_set()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  #[derive(Deserialize)]
  struct Args {
    key: String,
    path: String,
    data: String,
    nx: String,
  }
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(client) => client,
    Err(_) => return pj(&ECLIENT),
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return pj(&ECONN),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let j: Args = from_slice(cb).unwrap();
  let key: String = j.key;
  let path: String = j.path;
  let rdata: Vec<u8> = base64::decode(j.data.as_bytes()).unwrap();
  let data: String = String::from_utf8_lossy(&rdata).into_owned();
  let mut ret: &[u8] = &OK;
  match j.nx.as_str() {
    "false" => {
      let _: () = match redis::cmd("JSON.SET")
        .arg(key)
        .arg(path)
        .arg(data)
        .query::<String>(&mut con)
      {
        Ok(_) => {}
        Err(_) => {
          ret = &EQUERY;
        }
      };
      return pj(ret);
    }
    _ => {
      let _: () = match redis::cmd("JSON.SET")
        .arg(key)
        .arg(path)
        .arg(data)
        .arg("NX")
        .query::<String>(&mut con)
      {
        Ok(_) => {}
        Err(_) => {
          ret = &EQUERY;
        }
      };
      return pj(ret);
    }
  };
}

#[no_mangle]
pub extern "C" fn set(h: *const c_char, c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.set()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  #[derive(Deserialize)]
  struct Args {
    expire: String,
    data: HashMap<String, String>,
  }
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(client) => client,
    Err(_) => return pj(&ECONN),
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return pj(&ECONN),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let j: Args = from_slice(cb).unwrap();
  let d: HashMap<String, String> = j.data;
  let mut ret: &[u8] = &OK;
  match j.expire.as_str() {
    "0" => {
      for (k, v) in &d {
        let _: () = match redis::cmd("SET").arg(k).arg(v).query::<String>(&mut con) {
          Ok(_) => {}
          Err(_) => {
            ret = &ECONN;
            break;
          }
        };
      }
      return pj(ret);
    }
    _ => {
      for (k, v) in &d {
        let _: () = match redis::cmd("SET")
          .arg(k)
          .arg(v)
          .arg("EX")
          .arg(&j.expire)
          .query::<String>(&mut con)
        {
          Ok(_) => {}
          Err(_) => {
            ret = &EQUERY;
            break;
          }
        };
      }
      return pj(ret);
    }
  };
}

#[no_mangle]
pub extern "C" fn json_del(h: *const c_char, c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.json_del()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  #[derive(Deserialize)]
  struct Args {
    key: String,
    path: String,
  }
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(client) => client,
    Err(_) => return pj(&ECLIENT),
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return pj(&ECONN),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let j: Args = from_slice(cb).unwrap();
  let key: String = j.key;
  let path: String = j.path;
  let _: () = match redis::cmd("JSON.DEL")
    .arg(key)
    .arg(path)
    .query::<Vec<u8>>(&mut con)
  {
    Ok(_) => return pj(&OK),
    Err(_) => return pj(&EQUERY),
  };
}
