importScripts('https://cdn.sheetjs.com/xlsx-latest/package/dist/xlsx.full.min.js');

const CHUNK_SIZE = 5000;

self.addEventListener('message', function (e) {
    const data = e.data;

    if (data.action === 'processFile') {
        const file = data.file;
        const reader = new FileReader();

        reader.onload = function (event) {
            try {
                const arrayBuffer = event.target.result;
                self.postMessage({status: 'info', message: 'File loaded into memory. Parsing workbook...'});

                const workbook = XLSX.read(arrayBuffer, {type: 'array', cellFormula: true, cellHTML: false});
                const targetSheets = ['COMPANY_INFO', 'TAXPAID', 'PURCHASE', 'SALE', 'REVERSE_CHARGE'];
                let finalProcessedData = {};

                targetSheets.forEach((sheetName) => {
                    const ws = workbook.Sheets[sheetName];
                    if (!ws) return;

                    // CRITICAL FIX: { header: 1 } ensures strict column alignment (Array of Arrays)
                    const sheetData = XLSX.utils.sheet_to_json(ws, {header: 1, raw: false, defval: null});
                    const totalRows = sheetData.length;

                    self.postMessage({
                        status: 'progress',
                        sheet: sheetName,
                        processed: 0,
                        total: totalRows,
                        message: `Processing ${sheetName}...`,
                    });

                    let chunkedData = [];
                    for (let i = 0; i < totalRows; i++) {
                        chunkedData.push(sheetData[i]);

                        if (chunkedData.length === CHUNK_SIZE || i === totalRows - 1) {
                            self.postMessage({
                                status: 'chunk',
                                sheet: sheetName,
                                data: chunkedData,
                                processed: i + 1,
                                total: totalRows,
                            });
                            chunkedData = [];
                        }
                    }
                    finalProcessedData[sheetName] = totalRows;
                });

                self.postMessage({status: 'complete', summary: finalProcessedData});
            } catch (error) {
                self.postMessage({status: 'error', message: error.message});
            }
        };

        reader.onerror = function () {
            self.postMessage({status: 'error', message: 'Error reading file.'});
        };

        reader.readAsArrayBuffer(file);
    }
});
