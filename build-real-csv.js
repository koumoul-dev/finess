const fs = require('fs')
const path = require('path')
const iconv = require('iconv-lite')
const { Transform } = require('stream')
const byline = require('byline')
const endOfLine = require('os').EOL
const lambert93toWGPS = require('./lambert93toWGPS')

const readFile = fs.createReadStream(path.join(__dirname, 'etalab_cs1100507_stock_20180129-0428.csv'))
const decode = iconv.decodeStream('iso-8859-1')


const split = new Transform({
  objectMode: true,
  transform(item, encoding, callback) {
    callback(null, item.split(';'))
  }
})

const structureetHeader = [
  'Numéro FINESS ET', 'Numéro FINESS EJ', 'Raison sociale', 'Raison sociale longue', 'Complément de raison sociale',
  'Complément de distribution', 'Numéro de voie', 'Type de voie', 'Libellé de voie', 'Complément de voie', 'Lieu-dit / BP',
  'Code Commune', 'Département', 'Libellé département', 'Ligne d’acheminement (CodePostal+Lib commune)', 'Téléphone', 'Télécopie',
  'Catégorie d’établissement', 'Libelle catégorie d’établissement', 'Catégorie d’agrégat d’établissement', 'Libellé catégorie d’agrégat d’établissement',
   'Numéro de SIRET', 'Code APE', 'Code MFT', 'Libelle MFT', 'Code SPH', 'Libelle SPH', 'Date d’ouverture', 'Date d’autorisation', 'Date de mise à jour sur la structure', 'Champ inconnu']
const structureetFilter = new Transform({
  objectMode: true,
  transform(item, encoding, callback) {
    if(item[0] === 'structureet') {
      callback(null, item.slice(1))
    } else callback(null)
  }
})

const geolocalisationHeader = ['Numéro FINESS ET', 'Longitude', 'Latitude', 'Source des coordonnées', 'Date de mise à jour des coordonnées']
const geolocalisationFilter = new Transform({
  objectMode: true,
  transform(item, encoding, callback) {
    if(item[0] === 'geolocalisation') {
      callback(null, item.slice(1, 2).concat(lambert93toWGPS(...item.slice(2,4)),item.slice(4)))
    } else callback(null)
  }
})

const merge = (header) => {
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

const decodeInput = readFile.pipe(decode).pipe(byline()).pipe(split)
decodeInput.pipe(structureetFilter).pipe(merge(structureetHeader)).pipe(fs.createWriteStream(path.join(__dirname, 'structureet_20180129.csv')))
decodeInput.pipe(geolocalisationFilter).pipe(merge(geolocalisationHeader)).pipe(fs.createWriteStream(path.join(__dirname, 'geolocalisation_20180129.csv')))

const geolocalisations = fs.readFileSync(path.join(__dirname, 'geolocalisation_20180129.csv'), 'utf-8').split(endOfLine).map(l => l.split(','))
geolocalisations.shift()
const geolocDict = Object.assign({}, ...geolocalisations.map(l => ({[l[0]] : l.slice(1)})))

const etablissements = fs.createReadStream(path.join(__dirname, 'structureet_20180129.csv'), 'utf-8')
let removeHeader = true
etablissements.pipe(byline()).pipe(new Transform({
  objectMode: true,
  transform(item, encoding, callback) {
    if(removeHeader) {
      removeHeader = false
      callback(null)
    } else{
      const arr = item.split(',')
      callback(null, arr.concat(geolocDict[arr[0]]))
    }
  }
})).pipe(merge(structureetHeader.concat(geolocalisationHeader.slice(1)))).pipe(fs.createWriteStream(path.join(__dirname, 'etablissements_geolocalises_20180129.csv')))
