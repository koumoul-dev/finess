const { Transform } = require('stream')
const endOfLine = require('os').EOL

module.exports = (header) => {
  let headerEmitted = false
  return new Transform({
    objectMode: true,
    transform(item, encoding, callback) {
      let prefix = ''
      if(!headerEmitted){
        prefix += header.map(f => f.indexOf(',')>=0 ? `"${f}"` : f).join(',') + endOfLine
        headerEmitted = true
      }
      callback(null, prefix + item.map(f => f.indexOf(',')>=0 ? `"${f}"` : f).join(',') + endOfLine)
    }
  })
}
