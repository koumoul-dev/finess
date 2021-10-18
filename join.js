const { Transform } = require('stream')
const endOfLine = require('os').EOL

function replace(s, escape){
  if(!escape) return s
  else if (s.charAt(0) === '"' && s.charAt(s.length - 1) === '"') return s.slice(1, -1).replace(/\"/g, '""')
  else return s.replace(/\"/g, '""')
}

module.exports = (header, escape = false) => {
  let headerEmitted = false
  return new Transform({
    objectMode: true,
    transform(item, encoding, callback) {
      let prefix = ''
      if(!headerEmitted){
        prefix += header.map(f => f.indexOf(',')>=0 ? `"${f}"` : f).join(',') + endOfLine
        headerEmitted = true
      }
      callback(null, prefix + item.map(f => ((f.indexOf(',') >= 0 || f.slice(1, -1).indexOf('"') >= 0) && (f.charAt(0) !== '"' || f.charAt(f.length - 1) !== '"')) ? `"${replace(f, escape)}"` : replace(f, escape)).join(',') + endOfLine)
    }
  })
}
