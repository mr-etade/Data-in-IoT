// Global variables for simulations
let pyodide;
let simulationStates = {
    structured: { active: false, interval: null },
    unstructured: { active: false, interval: null },
    json: { active: false, interval: null },
    bigData: { active: false, interval: null },
    stream: { active: false, interval: null }
};

let dataCounters = {
    volume: 0,
    velocity: 10,
    structured: 0,
    unstructured: 0,
    semiStructured: 0,
    valid: 0,
    errors: 0,
    missing: 0,
    batchCount: 0,
    streamRate: 0
};

let charts = {};
let sensorData = [];

// Initialize everything when DOM loads
document.addEventListener('DOMContentLoaded', function() {
    initializePage();
    initPyodide();
});

async function initPyodide() {
    try {
        pyodide = await loadPyodide();
        await pyodide.loadPackage(['sqlite3', 'pandas']);
        console.log('Pyodide loaded successfully');
    } catch (error) {
        console.error('Failed to load Pyodide:', error);
        console.log('Using JavaScript fallback for SQL queries');
    }
}

function initializePage() {
    setupTabNavigation();
    setupDataTypeSimulations();
    setupBigDataSimulation();
    setupProcessingSimulations();
    setupSQLDemo();
    initializeCharts();
    generateInitialSQLData();
}

// Tab Navigation
function setupTabNavigation() {
    const tabButtons = document.querySelectorAll('.tab-button');
    const tabContents = document.querySelectorAll('.tab-content');

    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            const targetTab = button.getAttribute('data-tab');
            
            // Remove active class from all tabs and contents
            tabButtons.forEach(btn => btn.classList.remove('active'));
            tabContents.forEach(content => content.classList.remove('active'));
            
            // Add active class to clicked tab and corresponding content
            button.classList.add('active');
            document.getElementById(targetTab).classList.add('active');
        });
    });
}

// Data Types Simulations
function setupDataTypeSimulations() {
    // Structured Data Simulation
    document.getElementById('toggle-structured').addEventListener('click', toggleStructuredSimulation);
    document.getElementById('clear-structured').addEventListener('click', clearStructuredData);
    
    // Unstructured Data Simulation
    document.getElementById('toggle-unstructured').addEventListener('click', toggleUnstructuredSimulation);
    document.getElementById('clear-unstructured').addEventListener('click', clearUnstructuredData);
    
    // JSON Data Simulation
    document.getElementById('toggle-json').addEventListener('click', toggleJSONSimulation);
    document.getElementById('clear-json').addEventListener('click', clearJSONData);
}

function toggleStructuredSimulation() {
    const button = document.getElementById('toggle-structured');
    
    if (!simulationStates.structured.active) {
        simulationStates.structured.active = true;
        button.textContent = 'Stop Simulation';
        button.classList.remove('btn-primary');
        button.classList.add('btn-accent2');
        
        simulationStates.structured.interval = setInterval(() => {
            addStructuredDataRow();
        }, 2000);
    } else {
        stopStructuredSimulation();
    }
}

function stopStructuredSimulation() {
    simulationStates.structured.active = false;
    const button = document.getElementById('toggle-structured');
    button.textContent = 'Start Simulation';
    button.classList.remove('btn-accent2');
    button.classList.add('btn-primary');
    
    if (simulationStates.structured.interval) {
        clearInterval(simulationStates.structured.interval);
    }
}

function addStructuredDataRow() {
    const tbody = document.getElementById('sensor-tbody');
    const deviceIds = ['TEMP_001', 'TEMP_002', 'TEMP_003', 'HUM_001', 'HUM_002'];
    const statuses = ['Online', 'Online', 'Offline', 'Maintenance'];
    
    const row = document.createElement('tr');
    const timestamp = new Date().toISOString().slice(0, 19);
    const deviceId = deviceIds[Math.floor(Math.random() * deviceIds.length)];
    const temperature = (Math.random() * 35 + 15).toFixed(1);
    const humidity = (Math.random() * 80 + 20).toFixed(1);
    const status = statuses[Math.floor(Math.random() * statuses.length)];
    
    row.innerHTML = `
        <td>${timestamp}</td>
        <td>${deviceId}</td>
        <td>${temperature}</td>
        <td>${humidity}</td>
        <td><span class="status ${status.toLowerCase()}">${status}</span></td>
    `;
    
    tbody.insertBefore(row, tbody.firstChild);
    
    // Keep only last 10 rows
    while (tbody.children.length > 10) {
        tbody.removeChild(tbody.lastChild);
    }
    
    dataCounters.structured++;
    dataCounters.volume++;
}

function clearStructuredData() {
    document.getElementById('sensor-tbody').innerHTML = '';
}

function toggleUnstructuredSimulation() {
    const button = document.getElementById('toggle-unstructured');
    
    if (!simulationStates.unstructured.active) {
        simulationStates.unstructured.active = true;
        button.textContent = 'Stop Log Stream';
        button.classList.remove('btn-primary');
        button.classList.add('btn-accent2');
        
        simulationStates.unstructured.interval = setInterval(() => {
            addLogMessage();
        }, 1500);
    } else {
        stopUnstructuredSimulation();
    }
}

function stopUnstructuredSimulation() {
    simulationStates.unstructured.active = false;
    const button = document.getElementById('toggle-unstructured');
    button.textContent = 'Start Log Stream';
    button.classList.remove('btn-accent2');
    button.classList.add('btn-primary');
    
    if (simulationStates.unstructured.interval) {
        clearInterval(simulationStates.unstructured.interval);
    }
}

function addLogMessage() {
    const container = document.getElementById('log-messages');
    const logTypes = ['info', 'warning', 'error'];
    const messages = [
        'Device TEMP_001 temperature reading: 24.5°C',
        'Network connection established to gateway',
        'Battery level low on device HUM_002',
        'Data transmission completed successfully',
        'Sensor calibration required for PRESS_001',
        'Warning: High temperature detected in Zone A',
        'Error: Failed to connect to device TEMP_003',
        'Maintenance scheduled for device HUM_001',
        'Data backup completed to cloud storage',
        'Alert: Humidity threshold exceeded in Server Room'
    ];
    
    const logDiv = document.createElement('div');
    const logType = logTypes[Math.floor(Math.random() * logTypes.length)];
    const message = messages[Math.floor(Math.random() * messages.length)];
    const timestamp = new Date().toISOString();
    
    logDiv.className = `log-message ${logType}`;
    logDiv.innerHTML = `[${timestamp}] ${logType.toUpperCase()}: ${message}`;
    
    container.insertBefore(logDiv, container.firstChild);
    
    // Keep only last 15 messages
    while (container.children.length > 15) {
        container.removeChild(container.lastChild);
    }
    
    dataCounters.unstructured++;
    dataCounters.volume++;
}

function clearUnstructuredData() {
    document.getElementById('log-messages').innerHTML = '';
}

function toggleJSONSimulation() {
    const button = document.getElementById('toggle-json');
    
    if (!simulationStates.json.active) {
        simulationStates.json.active = true;
        button.textContent = 'Stop JSON Stream';
        button.classList.remove('btn-primary');
        button.classList.add('btn-accent2');
        
        simulationStates.json.interval = setInterval(() => {
            addJSONMessage();
        }, 3000);
    } else {
        stopJSONSimulation();
    }
}

function stopJSONSimulation() {
    simulationStates.json.active = false;
    const button = document.getElementById('toggle-json');
    button.textContent = 'Start JSON Stream';
    button.classList.remove('btn-accent2');
    button.classList.add('btn-primary');
    
    if (simulationStates.json.interval) {
        clearInterval(simulationStates.json.interval);
    }
}

function addJSONMessage() {
    const display = document.getElementById('json-display');
    const deviceTypes = ['temperature', 'humidity', 'pressure', 'motion', 'light'];
    const locations = ['Room A', 'Server Rack 1', 'Parking Lot', 'Main Entrance', 'Storage'];
    
    const jsonData = {
        timestamp: new Date().toISOString(),
        device_id: `${deviceTypes[Math.floor(Math.random() * deviceTypes.length)]}_${String(Math.floor(Math.random() * 999) + 1).padStart(3, '0')}`,
        location: locations[Math.floor(Math.random() * locations.length)],
        readings: {
            value: (Math.random() * 100).toFixed(2),
            unit: Math.random() > 0.5 ? '°C' : '%',
            quality: Math.random() > 0.1 ? 'good' : 'poor'
        },
        metadata: {
            firmware_version: `v${Math.floor(Math.random() * 3) + 1}.${Math.floor(Math.random() * 9)}.${Math.floor(Math.random() * 9)}`,
            battery_level: Math.floor(Math.random() * 100),
            signal_strength: Math.floor(Math.random() * 100)
        }
    };
    
    display.textContent = JSON.stringify(jsonData, null, 2);
    dataCounters.semiStructured++;
    dataCounters.volume++;
}

function clearJSONData() {
    document.getElementById('json-display').textContent = '';
}

// Big Data 4Vs Simulation
function setupBigDataSimulation() {
    document.getElementById('start-big-data').addEventListener('click', startBigDataSimulation);
    document.getElementById('stop-big-data').addEventListener('click', stopBigDataSimulation);
    document.getElementById('reset-big-data').addEventListener('click', resetBigDataCounters);
    document.getElementById('velocity-slider').addEventListener('input', updateVelocity);
}

function startBigDataSimulation() {
    if (!simulationStates.bigData.active) {
        simulationStates.bigData.active = true;
        
        simulationStates.bigData.interval = setInterval(() => {
            updateBigDataMetrics();
        }, 1000);
    }
}

function stopBigDataSimulation() {
    simulationStates.bigData.active = false;
    if (simulationStates.bigData.interval) {
        clearInterval(simulationStates.bigData.interval);
    }
}

function resetBigDataCounters() {
    dataCounters.volume = 0;
    dataCounters.structured = 0;
    dataCounters.unstructured = 0;
    dataCounters.semiStructured = 0;
    dataCounters.valid = 0;
    dataCounters.errors = 0;
    dataCounters.missing = 0;
    updateBigDataDisplay();
}

function updateVelocity(event) {
    dataCounters.velocity = parseInt(event.target.value);
    document.getElementById('velocity-rate').textContent = dataCounters.velocity;
}

function updateBigDataMetrics() {
    if (!simulationStates.bigData.active) return;
    
    // Simulate data generation based on velocity
    const recordsThisSecond = Math.floor(dataCounters.velocity * (0.8 + Math.random() * 0.4));
    
    for (let i = 0; i < recordsThisSecond; i++) {
        dataCounters.volume++;
        
        // Random data type generation
        const rand = Math.random();
        if (rand < 0.5) {
            dataCounters.structured++;
        } else if (rand < 0.8) {
            dataCounters.unstructured++;
        } else {
            dataCounters.semiStructured++;
        }
        
        // Simulate data quality issues
        const qualityRand = Math.random();
        if (qualityRand > 0.05) {
            dataCounters.valid++;
        } else if (qualityRand > 0.02) {
            dataCounters.errors++;
        } else {
            dataCounters.missing++;
        }
    }
    
    updateBigDataDisplay();
    updateCharts();
}

function updateBigDataDisplay() {
    document.getElementById('volume-count').textContent = dataCounters.volume.toLocaleString();
    document.getElementById('velocity-rate').textContent = dataCounters.velocity;
    
    const totalQuality = dataCounters.valid + dataCounters.errors + dataCounters.missing;
    const qualityPercent = totalQuality > 0 ? ((dataCounters.valid / totalQuality) * 100).toFixed(1) : 100;
    document.getElementById('quality-score').textContent = qualityPercent + '%';
}

// Processing Simulations
function setupProcessingSimulations() {
    document.getElementById('process-batch').addEventListener('click', processBatch);
    document.getElementById('toggle-stream').addEventListener('click', toggleStreamProcessing);
}

function processBatch() {
    const button = document.getElementById('process-batch');
    const batchQueue = document.getElementById('batch-queue');
    const batchCountSpan = document.getElementById('batch-count');
    
    if (dataCounters.batchCount === 0) {
        // Generate batch data
        for (let i = 0; i < 50; i++) {
            const item = document.createElement('div');
            item.className = 'batch-item';
            batchQueue.appendChild(item);
            dataCounters.batchCount++;
        }
        batchCountSpan.textContent = dataCounters.batchCount;
        button.textContent = 'Process Batch';
    } else {
        // Process batch
        button.textContent = 'Processing...';
        button.disabled = true;
        
        const items = batchQueue.querySelectorAll('.batch-item');
        let processed = 0;
        
        const processInterval = setInterval(() => {
            if (processed < items.length) {
                items[processed].style.backgroundColor = '#1cc549';
                processed++;
            } else {
                clearInterval(processInterval);
                setTimeout(() => {
                    batchQueue.innerHTML = '';
                    dataCounters.batchCount = 0;
                    batchCountSpan.textContent = '0';
                    button.textContent = 'Generate Batch';
                    button.disabled = false;
                }, 1000);
            }
        }, 50);
    }
}

function toggleStreamProcessing() {
    const button = document.getElementById('toggle-stream');
    const streamFlow = document.getElementById('stream-flow');
    const streamRateSpan = document.getElementById('stream-rate');
    
    if (!simulationStates.stream.active) {
        simulationStates.stream.active = true;
        button.textContent = 'Stop Stream';
        button.classList.remove('btn-accent4');
        button.classList.add('btn-accent2');
        
        simulationStates.stream.interval = setInterval(() => {
            createStreamParticle();
            dataCounters.streamRate = Math.floor(Math.random() * 50 + 25);
            streamRateSpan.textContent = dataCounters.streamRate;
        }, 200);
    } else {
        simulationStates.stream.active = false;
        button.textContent = 'Start Stream';
        button.classList.remove('btn-accent2');
        button.classList.add('btn-accent4');
        
        if (simulationStates.stream.interval) {
            clearInterval(simulationStates.stream.interval);
        }
        streamRateSpan.textContent = '0';
    }
}

function createStreamParticle() {
    const streamFlow = document.getElementById('stream-flow');
    const particle = document.createElement('div');
    particle.className = 'stream-particle';
    particle.style.top = Math.random() * 40 + 10 + 'px';
    
    streamFlow.appendChild(particle);
    
    setTimeout(() => {
        if (particle.parentNode) {
            particle.parentNode.removeChild(particle);
        }
    }, 2000);
}

// SQL Demo Setup
function setupSQLDemo() {
    document.getElementById('execute-sql').addEventListener('click', executeSQL);
    document.getElementById('sample-queries').addEventListener('click', toggleSampleQueries);
    document.getElementById('generate-data').addEventListener('click', generateSQLData);
    
    // Sample query buttons
    document.addEventListener('click', function(e) {
        if (e.target.classList.contains('query-btn')) {
            const query = e.target.getAttribute('data-query');
            document.getElementById('sql-input').value = query;
        }
    });
}

function toggleSampleQueries() {
    document.querySelector('.sample-queries').classList.toggle('hidden');
}

function generateInitialSQLData() {
    generateSQLData();
}

function generateSQLData() {
    sensorData = [];
    const deviceIds = ['TEMP_001', 'TEMP_002', 'HUM_001', 'HUM_002', 'PRESS_001'];
    const locations = ['Room A', 'Room B', 'Server Room', 'Warehouse', 'Office'];
    
    for (let i = 0; i < 100; i++) {
        const timestamp = new Date(Date.now() - Math.random() * 7 * 24 * 60 * 60 * 1000);
        sensorData.push({
            id: i + 1,
            timestamp: timestamp.toISOString().slice(0, 19).replace('T', ' '),
            device_id: deviceIds[Math.floor(Math.random() * deviceIds.length)],
            location: locations[Math.floor(Math.random() * locations.length)],
            temperature: (Math.random() * 35 + 10).toFixed(1),
            humidity: (Math.random() * 80 + 20).toFixed(1),
            pressure: (Math.random() * 200 + 1000).toFixed(0),
            battery_level: Math.floor(Math.random() * 100),
            status: Math.random() > 0.1 ? 'Online' : 'Offline'
        });
    }
    
    document.getElementById('sql-output').innerHTML = `<div style="color: #b9d4b4;">✓ Generated ${sensorData.length} sample IoT sensor records</div>`;
    document.getElementById('query-stats').innerHTML = `Dataset ready: ${sensorData.length} records available for querying`;
}

async function executeSQL() {
    const sqlInput = document.getElementById('sql-input').value.trim();
    const outputDiv = document.getElementById('sql-output');
    const statsDiv = document.getElementById('query-stats');
    
    if (!sqlInput) {
        outputDiv.innerHTML = '<div style="color: #f46659;">Please enter a SQL query</div>';
        return;
    }
    
    outputDiv.innerHTML = '<div class="loading">Executing query...</div>';
    
    try {
        let results;
        let executionTime;
        const startTime = performance.now();
        
        if (pyodide) {
            results = await executeSQLWithPyodide(sqlInput);
        } else {
            results = executeSQLWithJS(sqlInput);
        }
        
        executionTime = (performance.now() - startTime).toFixed(2);
        
        displaySQLResults(results, executionTime);
        
    } catch (error) {
        outputDiv.innerHTML = `<div style="color: #f46659;">Error: ${error.message}</div>`;
        statsDiv.innerHTML = 'Query failed';
    }
}

async function executeSQLWithPyodide(sql) {
    const code = `
import sqlite3
import pandas as pd
import json

conn = sqlite3.connect(':memory:')
df = pd.DataFrame(json.loads('${JSON.stringify(sensorData)}'))
df.to_sql('sensor_data', conn, index=False)
result = pd.read_sql_query('${sql.replace(/'/g, "\\'")}', conn)
conn.close()
result.to_json(orient='records')
`;
    const output = await pyodide.runPythonAsync(code);
    return JSON.parse(output);
}

function executeSQLWithJS(sql) {
    // Simple JavaScript-based SQL parser for demo purposes (limited functionality)
    const sqlLower = sql.toLowerCase().trim();
    
    if (sqlLower.startsWith('select')) {
        return parseSelectQuery(sql);
    } else {
        throw new Error('Only SELECT queries are supported in this demo');
    }
}

function parseSelectQuery(sql) {
    // Very basic parser: supports SELECT * / fields, FROM sensor_data, WHERE simple conditions, GROUP BY, ORDER BY, LIMIT
    // This is not a full SQL parser, just for demo
    let results = [...sensorData];
    
    // Parse fields
    let fields = '*';
    const selectMatch = sql.match(/select\s+(.*?)\s+from/i);
    if (selectMatch) {
        fields = selectMatch[1].trim().split(/\s*,\s*/).map(f => f.toLowerCase());
    }
    
    // Parse WHERE
    const whereMatch = sql.match(/where\s+(.*?)(\s+group|\s+order|\s+limit|$)/i);
    if (whereMatch) {
        const condition = whereMatch[1].trim();
        results = results.filter(row => evalCondition(row, condition));
    }
    
    // Parse GROUP BY (basic, for AVG)
    const groupMatch = sql.match(/group by\s+(.*?)(\s+order|\s+limit|$)/i);
    if (groupMatch) {
        const groupField = groupMatch[1].trim().toLowerCase();
        results = groupBy(results, groupField, fields);
    }
    
    // Parse ORDER BY
    const orderMatch = sql.match(/order by\s+(.*?)(\s+desc|\s+asc)?(\s+limit|$)/i);
    if (orderMatch) {
        const orderField = orderMatch[1].trim().toLowerCase();
        const direction = (orderMatch[2] || '').toLowerCase() === 'desc' ? -1 : 1;
        results.sort((a, b) => (a[orderField] > b[orderField] ? 1 : -1) * direction);
    }
    
    // Parse LIMIT
    const limitMatch = sql.match(/limit\s+(\d+)/i);
    if (limitMatch) {
        results = results.slice(0, parseInt(limitMatch[1]));
    }
    
    // Select fields
    if (fields !== '*') {
        results = results.map(row => {
            const newRow = {};
            fields.forEach(f => {
                if (f in row) newRow[f] = row[f];
            });
            return newRow;
        });
    }
    
    return results;
}

function evalCondition(row, condition) {
    // Basic condition eval: supports >, <, =, AND, OR, field names as is
    // Security note: For demo only, not safe for production
    condition = condition.replace(/(\w+)\s*(>|<|=|>=|<=|!=)\s*(\d+\.?\d*)/g, 'row["$1"] $2 $3');
    condition = condition.replace(/and/gi, '&&');
    condition = condition.replace(/or/gi, '||');
    try {
        return eval(condition);
    } catch (e) {
        throw new Error('Invalid WHERE condition');
    }
}

function groupBy(data, groupField, fields) {
    // Basic group by for AVG
    const groups = {};
    data.forEach(row => {
        const key = row[groupField];
        if (!groups[key]) groups[key] = { count: 0, sums: {}, group: key };
        groups[key].count++;
        fields.forEach(f => {
            if (f.startsWith('avg(')) {
                const sumField = f.match(/avg\((.*?)\)/)[1].toLowerCase();
                groups[key].sums[sumField] = (groups[key].sums[sumField] || 0) + parseFloat(row[sumField]);
            }
        });
    });
    
    return Object.values(groups).map(g => {
        const result = { [groupField]: g.group };
        Object.keys(g.sums).forEach(sumField => {
            const avgField = `avg_${sumField}`;
            result[avgField] = (g.sums[sumField] / g.count).toFixed(2);
        });
        return result;
    });
}

function displaySQLResults(results, time) {
    const outputDiv = document.getElementById('sql-output');
    const statsDiv = document.getElementById('query-stats');
    
    if (results.length === 0) {
        outputDiv.innerHTML = '<div style="color: #b9d4b4;">No results found</div>';
        statsDiv.innerHTML = `0 rows returned in ${time} ms`;
        return;
    }
    
    // Create table
    const table = document.createElement('table');
    table.className = 'sql-results-table';
    
    // Headers
    const headers = Object.keys(results[0]);
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    headers.forEach(h => {
        const th = document.createElement('th');
        th.textContent = h;
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);
    
    // Body
    const tbody = document.createElement('tbody');
    results.forEach(row => {
        const tr = document.createElement('tr');
        headers.forEach(h => {
            const td = document.createElement('td');
            td.textContent = row[h];
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    
    outputDiv.innerHTML = '';
    outputDiv.appendChild(table);
    
    statsDiv.innerHTML = `${results.length} rows returned in ${time} ms`;
}

// Charts
function initializeCharts() {
    charts.variety = document.getElementById('variety-chart').getContext('2d');
    charts.veracity = document.getElementById('veracity-chart').getContext('2d');
}

function updateCharts() {
    drawPie(charts.variety, [dataCounters.structured, dataCounters.unstructured, dataCounters.semiStructured], ['#b9d4b4', '#f46659', '#ffbc3e']);
    drawBar(charts.veracity, [dataCounters.valid, dataCounters.errors, dataCounters.missing], ['#1cc549', '#f46659', '#ffbc3e']);
}

function drawPie(ctx, data, colors) {
    const width = ctx.canvas.width;
    const height = ctx.canvas.height;
    const centerX = width / 2;
    const centerY = height / 2;
    const radius = Math.min(width, height) / 2 - 10;
    
    let total = data.reduce((a, b) => a + b, 0);
    let startAngle = 0;
    
    ctx.clearRect(0, 0, width, height);
    
    data.forEach((value, i) => {
        if (value === 0) return;
        const angle = (value / total) * 2 * Math.PI;
        
        ctx.beginPath();
        ctx.moveTo(centerX, centerY);
        ctx.arc(centerX, centerY, radius, startAngle, startAngle + angle);
        ctx.closePath();
        ctx.fillStyle = colors[i];
        ctx.fill();
        
        startAngle += angle;
    });
}

function drawBar(ctx, data, colors) {
    const width = ctx.canvas.width;
    const height = ctx.canvas.height;
    const barWidth = width / data.length / 1.5;
    const maxValue = Math.max(...data) || 1;
    
    ctx.clearRect(0, 0, width, height);
    
    data.forEach((value, i) => {
        const barHeight = (value / maxValue) * (height - 20);
        ctx.fillStyle = colors[i];
        ctx.fillRect(i * width / data.length + (width / data.length - barWidth) / 2, height - barHeight, barWidth, barHeight);
    });
}