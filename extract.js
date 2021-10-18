const fs = require('fs')
const path = require('path')
const iconv = require('iconv-lite')
const { Transform } = require('stream')
const byline = require('byline')
const streamToPromise = require('stream-to-promise')

const decode = iconv.decodeStream('iso-8859-1')

const split = new Transform({
  objectMode: true,
  transform(item, encoding, callback) {
    callback(null, item.split(';').map(l => {
      const s = l.replace(/\"\"\"\"/g, '""')
      // if (s.charAt(0) === '"' && s.charAt(s.length - 1) === '"') return s.slice(1, -1)
      return s
    }))
  }
})

const structureetHeader = [
  'Numéro FINESS ET', 'Numéro FINESS EJ', 'Raison sociale', 'Raison sociale longue', 'Complément de raison sociale',
  'Complément de distribution', 'Numéro de voie', 'Type de voie', 'Libellé de voie', 'Complément de voie', 'Lieu-dit / BP',
  'Code Commune', 'Département', 'Libellé département', 'Ligne d’acheminement (CodePostal+Lib commune)', 'Téléphone', 'Télécopie',
  'Catégorie d’établissement', 'Libelle catégorie d’établissement', 'Catégorie d’agrégat d’établissement', 'Libellé catégorie d’agrégat d’établissement',
   'Numéro de SIRET', 'Code APE', 'Code MFT', 'Libelle MFT', 'Code SPH', 'Libelle SPH', 'Date d’ouverture', 'Date d’autorisation', 'Date de mise à jour sur la structure', 'code UAI (RNE)']
const structureetFilter = new Transform({
  objectMode: true,
  transform(item, encoding, callback) {
    if(item[0] === 'structureet') {
      callback(null, item.slice(1))
    } else callback(null)
  }
})

const geolocalisationHeader = ['Numéro FINESS ET', 'X', 'Y', 'Source des coordonnées', 'Date de mise à jour des coordonnées']
const geolocalisationFilter = new Transform({
  objectMode: true,
  transform(item, encoding, callback) {
    if(item[0] === 'geolocalisation') {
      callback(null, item.slice(1))
    } else callback(null)
  }
})
const join = require('./join')

module.exports = async () => {
  console.log('Extracting etablissements and geolocation files')
  const readFile = fs.createReadStream(path.join(__dirname, 'etalab-cs1100507-stock-20210913-0412.csv'))
  const decodeInput = readFile.pipe(decode).pipe(byline()).pipe(split)

  const out1 = fs.createWriteStream(path.join(__dirname, 'structureet_20210913.csv'))
  const out2 = fs.createWriteStream(path.join(__dirname, 'geolocalisation_20210913.csv'))
  decodeInput.pipe(structureetFilter).pipe(join(structureetHeader)).pipe(out1)
  decodeInput.pipe(geolocalisationFilter).pipe(join(geolocalisationHeader)).pipe(out2)

  return Promise.all([streamToPromise(out1), streamToPromise(out2)])
}
