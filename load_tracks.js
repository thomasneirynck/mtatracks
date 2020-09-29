const {Client} = require('@elastic/elasticsearch');
const fs = require('fs');
const readline = require('readline');
const turf = require('@turf/turf');
const yargs = require('yargs')


const DEFAULT_TRACKS_JSON = 'manhattan_tracks.json';
const DEFAULT_INDEX_NAME = 'tracks';
const DEFAULT_UPDATE_DELTA = 1000; //ms
const DEFAULT_SPEED = 40; //mph
const DEFAULT_HOST = `https://localhost:9200`;
const distanceUnit = 'miles';

const argv = yargs
    .option('index', {
        alias: 'i',
        description: 'name of the elasticsearch index',
        type: 'string',
        default: DEFAULT_INDEX_NAME,
    })
    .option('frequency', {
        alias: 'f',
        description: `Update delta of the tracks in ms`,
        type: 'number',
        default: DEFAULT_UPDATE_DELTA,
    })
    .option('host', {
        alias: 'h',
        description: 'URL of the elasticsearch server',
        type: 'string',
        default: DEFAULT_HOST,
    })
    .help()
    .argv;

const tracksIndexName = argv.index;
const updateDelta = argv.frequency; //milliseconds

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const esClient = new Client({
    node: argv.host,
    auth: {
        username: 'elastic',
        password: 'changeme'
    },
    ssl: {
        rejectUnauthorized: false
    }
});

async function init() {
    initTrackMeta();
    await setupIndex();
    generateWaypoints();
}

init();

function initTrackMeta() {
}

async function recreateIndex() {
    console.log(`Create index "${tracksIndexName}"`);
    try {
        await esClient.indices.create({
            index: tracksIndexName,
            body: {
                mappings: {
                    "properties": {
                        'location': {
                            "type": 'geo_point',
                            "ignore_malformed": true
                        },
                        "entity_id": {
                            "type": "keyword"
                        },
                        "azimuth": {
                            "type": "double"
                        },
                        "speed": {
                            "type": "double"
                        },
                        "@timestamp": {
                            "type": "date"
                        }
                    }
                }
            }
        });
    } catch (e) {
        console.error(e.body.error);
        throw e;
    }
}

async function setupIndex() {

    return new Promise(async (resolve, reject) => {

        try {
            await esClient.ping({});
        } catch (e) {
            console.error('Cannot reach Elasticsearch', e);
            reject(e);
        }

        try {

            const {body} = await esClient.indices.exists({
                index: tracksIndexName,
            });

            if (body) {
                rl.question(`Index "${tracksIndexName}" exists. Should delete and recreate? [n|Y]`, async function (response) {
                    if (response === 'y' || response === 'Y') {
                        console.log(`Deleting index "${tracksIndexName}"`);
                        await esClient.indices.delete({
                            index: tracksIndexName
                        });
                        await recreateIndex();
                    } else {
                        console.log('Retaining existing index');
                    }
                    resolve();
                });

            } else {
                await recreateIndex();
                resolve();
            }

        } catch (e) {
            console.error(e.message);
            reject(e);
        }
    });
}

let tickCounter = 0;

async function generateWaypoints() {

    console.log(`[${tickCounter}-------------- GENERATE ${tracksFeatureCollection.features.length} WAYPOINTS AT TICK ${(new Date()).toISOString()}`);


    tickCounter++;
    setTimeout(generateWaypoints, updateDelta);

}