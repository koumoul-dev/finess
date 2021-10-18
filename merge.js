const fs = require('fs')
const path = require('path')
const { Transform } = require('stream')
const byline = require('byline')
const endOfLine = require('os').EOL
const streamToPromise = require('stream-to-promise')
const csv = require('csv')
const parse = require('csv-parse/lib/sync')

const proj4 = require('proj4')
// This projections come from https://epsg.io/
proj4.defs("EPSG:2154","+proj=lcc +lat_1=49 +lat_2=44 +lat_0=46.5 +lon_0=3 +x_0=700000 +y_0=6600000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs");
proj4.defs("EPSG:4559","+proj=utm +zone=20 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs")
proj4.defs("EPSG:2972","+proj=utm +zone=22 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs");
proj4.defs("EPSG:2975","+proj=utm +zone=40 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs");
proj4.defs("EPSG:4467","+proj=utm +zone=21 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs");
proj4.defs("EPSG:4471","+proj=utm +zone=38 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs");
proj4.defs("EPSG:4326","+proj=longlat +datum=WGS84 +no_defs");

const dep2epsg = {
  'metro': 'EPSG:2154',
  '9A': 'EPSG:4559',
  '9B': 'EPSG:4559',
  '9C': 'EPSG:2972',
  '9D': 'EPSG:2975',
  '9E': 'EPSG:4467',
  '9F': 'EPSG:4471'
}

const headers = ['Numéro FINESS ET', 'Numéro FINESS EJ', 'Raison sociale', 'Raison sociale longue', 'Complément de raison sociale',
'Complément de distribution', 'Numéro de voie', 'Type de voie', 'Libellé de voie', 'Complément de voie', 'Lieu-dit / BP',
'Code Commune', 'Département', 'Libellé département', 'Ligne d’acheminement (CodePostal+Lib commune)', 'Téléphone', 'Télécopie',
'Catégorie d’établissement', 'Libelle catégorie d’établissement', 'Catégorie d’agrégat d’établissement', 'Libellé catégorie d’agrégat d’établissement',
 'Numéro de SIRET', 'Code APE', 'Code MFT', 'Libelle MFT', 'Code SPH', 'Libelle SPH', 'Date d’ouverture', 'Date d’autorisation', 'Date de mise à jour sur la structure',
 'code UAI (RNE)', 'Longitude', 'Latitude', 'Source des coordonnées', 'Date de mise à jour des coordonnées', 'Adresse']
const join = require('./join')

module.exports = async () => {
  console.log('Merging etablissements and geolocation files')
  const geolocalisations = parse(fs.readFileSync(path.join(__dirname, 'geolocalisation_20210913.csv'), 'utf-8')) //.split(endOfLine).map(l => l.split(','))
  geolocalisations.shift()
  const geolocDict = Object.assign({}, ...geolocalisations.map(l => ({[l[0]] : l.slice(1)})))

  const etablissements = fs.createReadStream(path.join(__dirname, 'structureet_20210913.csv'), 'utf-8')
  const out = fs.createWriteStream(path.join(__dirname, 'etablissements_geolocalises_20210913.csv'))
  let removeHeader = true
  etablissements.pipe(csv.parse()).pipe(new Transform({
    objectMode: true,
    transform(item, encoding, callback) {
      if(removeHeader) {
        removeHeader = false
        callback(null)
      } else {
        if (item.length !== 31) console.log(item.length)
        const geolocLine = geolocDict[item[0]]
        const reproject = proj4(dep2epsg[item[12]] || dep2epsg.metro, 'EPSG:4326', geolocDict[item[0]].slice(0,2).map(c => Number(c))).map(c => c +'')
        callback(null, item.concat(reproject, geolocLine.slice(2), (item[6] + ' ' + item[7] + ' ' + item[8] + ' ' + item[14]).trim()))
      }
    }
  })).pipe(join(headers, true)).pipe(out)
  return streamToPromise(out)
}
