use std::ffi::{CStr};
use std::os::raw::{c_int, c_char, c_uchar};
use std::{slice, panic};

extern crate base64;
extern crate redis;
extern crate serde_json;
use serde::Deserialize;
use serde_json::from_slice;
use std::collections::HashMap;

const ECLIENT: c_int = -43;
const ECONN: c_int = -13;
const EINVALID: c_int = -9;
const EQUERY: c_int = -4;
const OK: c_int = 0;

fn buf(r: Vec<u8>, ptr: *mut c_uchar) -> c_int {
  let bytes = r.as_slice();
  let size = bytes.len();
  let got = size as c_int;
  unsafe {
    let o = slice::from_raw_parts_mut(ptr, size as usize);
    o[..size].copy_from_slice(&bytes);
  }
  return got;
}

#[no_mangle]
pub extern "C" fn del(h: *const c_char, c: *const c_char) -> c_int {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.del()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(rc) => rc,
    Err(_) => return ECLIENT,
  };
  let mut con = match client.get_connection() {
    Ok(rc) => rc,
    Err(_) => return ECONN,
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("DEL").arg(cb).query::<i32>(&mut con) {
    Ok(_) => return OK,
    Err(_) => return EQUERY,
  };
}

#[no_mangle]
pub extern "C" fn unlink(h: *const c_char, c: *const c_char) -> c_int {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.unlink()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(rc) => rc,
    Err(_) => return ECLIENT,
  };
  let mut con = match client.get_connection() {
    Ok(rc) => rc,
    Err(_) => return ECONN,
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("UNLINK").arg(cb).query::<i32>(&mut con) {
    Ok(_) => return OK,
    Err(_) => return EQUERY,
  };
}

#[no_mangle]
pub extern "C" fn incr(h: *const c_char, c: *const c_char) -> c_int {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.incr()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(rc) => rc,
    Err(_) => return ECLIENT,
  };
  let mut con = match client.get_connection() {
    Ok(rc) => rc,
    Err(_) => return ECONN,
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("INCR").arg(cb).query::<i32>(&mut con) {
    Ok(_) => return OK,
    Err(_) => return EQUERY,
  };
}

#[no_mangle]
pub extern "C" fn json_get(h: *const c_char, c: *const c_char, b: *mut c_uchar) -> c_int {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.json_get()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  #[derive(Deserialize)]
  struct Args {
    key: String,
    path: String,
  }
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(client) => client,
    Err(_) => return ECLIENT,
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return ECONN,
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
    Ok(s) => {
      return buf(s, b);
    }
    Err(_) => return EQUERY,
  };
}

#[no_mangle]
pub extern "C" fn get(h: *const c_char, c: *const c_char, b: *mut c_uchar) -> c_int {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.get()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(client) => client,
    Err(_) => return ECLIENT,
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return ECONN,
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("GET").arg(cb).query::<Vec<u8>>(&mut con) {
    Ok(s) => {
      return buf(s, b);
    }
    Err(_) => return EQUERY,
  };
}

#[no_mangle]
pub extern "C" fn hget(h: *const c_char, c: *const c_char, b: *mut c_uchar) -> c_int {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.json_get()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  #[derive(Deserialize)]
  struct Args {
    hash: String,
    field: String,
  }
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(client) => client,
    Err(_) => return ECLIENT,
  };
  let mut conn = match client.get_connection() {
    Ok(conn) => conn,
    Err(_) => return ECONN,
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let j: Args = match from_slice(cb) {
    Ok(j) => j,
    Err(_) => return EINVALID,
  };
  let hash: String = j.hash;
  let field: String = j.field;
  let _: () = match redis::cmd("HGET")
    .arg(hash)
    .arg(field)
    .query::<Vec<u8>>(&mut conn)
  {
    Ok(s) => return buf(s, b),
    Err(_) => return EQUERY,
  };
}

#[no_mangle]
pub extern "C" fn json_set(h: *const c_char, c: *const c_char) ->  c_int {
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
    Err(_) => return ECLIENT,
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return ECONN,
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let j: Args = from_slice(cb).unwrap();
  let key: String = j.key;
  let path: String = j.path;
  let rdata: Vec<u8> = base64::decode(j.data.as_bytes()).unwrap();
  let data: String = String::from_utf8_lossy(&rdata).into_owned();
  match j.nx.as_str() {
    "false" => {
      let _: () = match redis::cmd("JSON.SET")
        .arg(key)
        .arg(path)
        .arg(data)
        .query::<String>(&mut con)
      {
        Ok(_) => {
          return OK;
        }
        Err(_) => {
          return EQUERY;
        }
      };
    }
    _ => {
      let _: () = match redis::cmd("JSON.SET")
        .arg(key)
        .arg(path)
        .arg(data)
        .arg("NX")
        .query::<String>(&mut con)
      {
        Ok(_) => {
          return OK;
        }
        Err(_) => {
          return EQUERY;
        }
      };
    }
  };
}

#[no_mangle]
pub extern "C" fn hset(h: *const c_char, c: *const c_char) -> c_int {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.hset()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(client) => client,
    Err(_) => return ECLIENT,
  };
  let mut conn = match client.get_connection() {
    Ok(conn) => conn,
    Err(_) => return ECONN,
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let j: HashMap<String, HashMap<String, String>> = match from_slice(cb) {
    Ok(j) => j,
    Err(_) => return EINVALID,
  };
  let mut ret: c_int = OK;
  for (hash, map) in &j {
    for (k, v) in map {
      let _: () = match redis::cmd("HSET")
        .arg(hash)
        .arg(k)
        .arg(v)
        .query::<i32>(&mut conn)
      {
        Ok(_) => {}
        Err(_) => {
          ret = EQUERY;
        }
      };
    }
  }
  return ret;
}

#[no_mangle]
pub extern "C" fn set(h: *const c_char, c: *const c_char) -> c_int {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.set()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  #[derive(Deserialize)]
  struct Args {
    expire: String,
    data: HashMap<String, String>,
  }
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(client) => client,
    Err(_) => return ECLIENT,
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return ECONN,
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let j: Args = from_slice(cb).unwrap();
  let d: HashMap<String, String> = j.data;
  let mut ret: c_int = OK;
  match j.expire.as_str() {
    "0" => {
      for (k, v) in &d {
        let _: () = match redis::cmd("SET").arg(k).arg(v).query::<String>(&mut con) {
          Ok(_) => {}
          Err(_) => {
            ret = EQUERY;
          }
        };
      }
      return ret;
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
            ret = EQUERY;
            break;
          }
        };
      }
      return ret;
    }
  };
}

#[no_mangle]
pub extern "C" fn json_del(h: *const c_char, c: *const c_char) -> c_int {
  panic::set_hook(Box::new(move |_| eprintln!("panic: redis.json_del()")));
  let ch = unsafe { CStr::from_ptr(h).to_str().unwrap() };
  #[derive(Deserialize)]
  struct Args {
    key: String,
    path: String,
  }
  let client = match redis::Client::open(format!("redis://{}/", &ch)) {
    Ok(client) => client,
    Err(_) => return ECLIENT,
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return ECONN,
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
    Ok(_) => return OK,
    Err(_) => return EQUERY,
  };
}
