
class Codec {

  async encode(message) {
    return JSON.stringify(message);
  }

  async decode(message) {
    return JSON.parse(message)
  }

}

module.exports = Codec;