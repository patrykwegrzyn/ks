
class Codec {

  encode(message) {
    const e = JSON.stringify(message);
    return Promise.resolve(e);
  }

  decode() {
    const d = JSON.Parse(message)
    return Promise.resolve(d);
  }

}