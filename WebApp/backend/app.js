const express = require('express');

const hbase = require('hbase');
const krb5 = require('krb5');
const fs = require('fs');
const cors = require("cors");

const app = express();
const port = 3000;

const client = createHBaseClient();

app.get('/api/get/:key', async (req, res) => {
    const { key } = req.params;

    if (!key) {
        return res.status(400).json({ error: 'Key is required' });
    }
    client.table('fmessaoud:clashgametable').scan({
        startRow: key,
        endRow: key,
        maxVersions: 1
    }, (error, cells) => {
        if (error) {
            console.error(error);
            return res.status(500).json({ error: 'Internal Server Error' });
        } else {
            // Convert the cells array to a JSON string with indentation for readability
            const firstCellValue = cells[0]['$'];
            const jsonResult = JSON.parse(firstCellValue);

            // Log the JSON string to the console
            console.log(jsonResult);
            res.status(200);
            res.setHeader('Access-Control-Allow-Origin', '*');
            res.json({data: jsonResult});
            //console.log(res);
            return res;
            //res.json({headers: 'Access-Control-Allow-Origin: *',body: jsonResult});

            // Save the JSON string to a file
            fs.writeFile('cells.json', JSON.stringify(jsonResult), 'utf8', (writeError) => {
                if (writeError) {
                    console.error('Error writing JSON to file:', writeError);
                } else {
                    console.log('JSON data saved to cells.json');
                }
            });
        }
    });
});

function createHBaseClient() {
    const client = hbase({
        host: 'lsd-prod-namenode-0.lsd.novalocal',
        protocol: 'https',
        port: 8080,
        krb5: {
            service_principal: 'HTTP/lsd-prod-namenode-0.lsd.novalocal',
            principal: 'fmessaoud@LSD.NOVALOCAL'
        }
    });
    return client;
}

function scanFirstCell() {
    client.table('fmessaoud:clashgametable').scan({
        startRow: '',
        endRow: '',
        maxVersions: 1
    }, (error, cells) => {
        if (error) {
            console.error(error);
        } else {
            // Convert the cells array to a JSON string with indentation for readability
            const firstCellValue = cells[0]['$'];
            const jsonResult = JSON.parse(firstCellValue);

            // Log the JSON string to the console
            console.log(jsonResult);

            // Save the JSON string to a file
            fs.writeFile('cells.json', JSON.stringify(jsonResult), 'utf8', (writeError) => {
                if (writeError) {
                    console.error('Error writing JSON to file:', writeError);
                } else {
                    console.log('JSON data saved to cells.json');
                }
            });
        }
    });
}

app.use(cors({origin: '*'}));
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});