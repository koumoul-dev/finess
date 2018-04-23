# Extract Finess data in a (real) CSV file
This work is still in progress

## Download files
 * clone this repository
 * Download data from [this page](https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/), take the file named `Extraction Finess des Etablissements Geolocalisés au 29/01/2018Extraction Finess des Etablissements Geolocalisés au 29/01/2018`

## Install libraries and run the script
You will need a recent NodeJS version (tested with version 8, should work with 6 and above)

```
npm install
node build-real-csv.js
```

The output is the file `etablissements_geolocalises_20180129.csv`

## TODO
 * handle projections for DOM TOM, see the download page for more details
 * Reorganize this repo
 * Find what this last column is
