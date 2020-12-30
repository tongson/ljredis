use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic;

extern crate base64;
extern crate redis;
extern crate serde_json;
use serde::Deserialize;
use serde_json::from_slice;
use std::collections::HashMap;

const HOST: &str = "127.0.0.1";

fn cs(s: Vec<u8>) -> *const c_char {
  let c_str = CString::new(s).unwrap();
  let ptr = c_str.as_ptr();
  std::mem::forget(c_str);
  return ptr;
}

#[no_mangle]
pub extern "C" fn del(c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.del()")));
  let ack: Vec<u8> = vec![6];
  let dc2: Vec<u8> = vec![18];
  let dc4: Vec<u8> = vec![20];
  let nak: Vec<u8> = vec![21];
  let client = match redis::Client::open(format!("redis://{}/", HOST)) {
    Ok(rc) => rc,
    Err(_) => return cs(dc2),
  };
  let mut con = match client.get_connection() {
    Ok(rc) => rc,
    Err(_) => return cs(dc4),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("DEL").arg(cb).query::<i32>(&mut con) {
    Ok(_) => return cs(ack),
    Err(_) => return cs(nak),
  };
}

#[no_mangle]
pub extern "C" fn unlink(c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.unlink()")));
  let ack: Vec<u8> = vec![6];
  let dc2: Vec<u8> = vec![18];
  let dc4: Vec<u8> = vec![20];
  let nak: Vec<u8> = vec![21];
  let client = match redis::Client::open(format!("redis://{}/", HOST)) {
    Ok(rc) => rc,
    Err(_) => return cs(dc2),
  };
  let mut con = match client.get_connection() {
    Ok(rc) => rc,
    Err(_) => return cs(dc4),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("UNLINK").arg(cb).query::<i32>(&mut con) {
    Ok(_) => return cs(ack),
    Err(_) => return cs(nak),
  };
}

#[no_mangle]
pub extern "C" fn incr(c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.incr()")));
  let ack: Vec<u8> = vec![6];
  let dc2: Vec<u8> = vec![18];
  let dc4: Vec<u8> = vec![20];
  let nak: Vec<u8> = vec![21];
  let client = match redis::Client::open(format!("redis://{}/", HOST)) {
    Ok(rc) => rc,
    Err(_) => return cs(dc2),
  };
  let mut con = match client.get_connection() {
    Ok(rc) => rc,
    Err(_) => return cs(dc4),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("INCR").arg(cb).query::<i32>(&mut con) {
    Ok(_) => return cs(ack),
    Err(_) => return cs(nak),
  };
}

#[no_mangle]
pub extern "C" fn json_get(c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.json_get()")));
  let nak: Vec<u8> = vec![21];
  let dc2: Vec<u8> = vec![18];
  let dc4: Vec<u8> = vec![20];
  #[derive(Deserialize)]
  struct Args {
    key: String,
    path: String,
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
  let key: String = j.key;
  let path: String = j.path;
  let _: () = match redis::cmd("JSON.GET").arg(key).arg(path).query::<Vec<u8>>(&mut con) {
    Ok(s) => return cs(s),
    Err(_) => return cs(nak),
  };
}

#[no_mangle]
pub extern "C" fn get(c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.get()")));
  let nak: Vec<u8> = vec![21];
  let dc2: Vec<u8> = vec![18];
  let dc4: Vec<u8> = vec![20];
  let client = match redis::Client::open(format!("redis://{}/", HOST)) {
    Ok(client) => client,
    Err(_) => return cs(dc2),
  };
  let mut con = match client.get_connection() {
    Ok(con) => con,
    Err(_) => return cs(dc4),
  };
  let cb = unsafe { CStr::from_ptr(c).to_bytes() };
  let _: () = match redis::cmd("GET").arg(cb).query::<Vec<u8>>(&mut con) {
    Ok(s) => return cs(s),
    Err(_) => return cs(nak),
  };
}

#[no_mangle]
pub extern "C" fn json_set(c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.set()")));
  let dc2: Vec<u8> = vec![18];
  let dc4: Vec<u8> = vec![20];
  #[derive(Deserialize)]
  struct Args {
    key: String,
    path: String,
    data: String,
    nx: String,
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
  let key: String = j.key;
  let path: String = j.path;
  let rdata: Vec<u8> = base64::decode(j.data.as_bytes()).unwrap();
  let data: String = String::from_utf8_lossy(&rdata).into_owned();
  let mut ret: Vec<u8> = vec![6];
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
          ret = vec![21];
        }
      };
      return cs(ret);
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
          ret = vec![21];
        }
      };
      return cs(ret);
    }
  };
}

#[no_mangle]
pub extern "C" fn set(c: *const c_char) -> *const c_char {
  panic::set_hook(Box::new(move |_| eprintln!("panic: rediz.set()")));
  let dc2: Vec<u8> = vec![18];
  let dc4: Vec<u8> = vec![20];
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
  let mut ret: Vec<u8> = vec![6];
  match j.expire.as_str() {
    "0" => {
      for (k, v) in &d {
        let _: () = match redis::cmd("SET").arg(k).arg(v).query::<String>(&mut con) {
          Ok(_) => {}
          Err(_) => {
            ret = vec![21];
            break;
          }
        };
      }
      return cs(ret);
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
            ret = vec![21];
            break;
          }
        };
      }
      return cs(ret);
    }
  };
}
