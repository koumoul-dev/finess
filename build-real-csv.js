const extract = require('./extract')
const merge = require('./merge')

extract().then(result => merge().then(results => console.log('End')))
