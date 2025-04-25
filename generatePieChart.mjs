import fs from 'fs';
import readline from 'readline';
import dotenv from 'dotenv';
import { ChartJSNodeCanvas } from 'chartjs-node-canvas';
import ChartDataLabels from 'chartjs-plugin-datalabels';
import path from 'path';

dotenv.config();

const width = 1200; // wider to prevent legend cut-off
const height = 1000;
const MAX_PRODUCTS = 10;
const FILE_PATH = './enriched_profiles2.ndjson';
const OUTPUT_PATH = './pie_chart_official_1.png';
const ESTIMATED_TOTAL_LINES = 108000;
const SEGMENT_NAME = process.env.KLAVIYO_SEGMENT_NAME || 'Unknown Segment';

const chartJSNodeCanvas = new ChartJSNodeCanvas({
  width,
  height,
  backgroundColour: 'white',
  chartCallback: (ChartJS) => {
    ChartJS.register(ChartDataLabels);
    ChartJS.defaults.color = '#004175';
    ChartJS.defaults.font.weight = 'bold';
  }
});

function renderProgress(current, done = false) {
  const percent = Math.min((current / ESTIMATED_TOTAL_LINES) * 100, 100);
  const filled = Math.floor((percent / 100) * 40);
  const bar = '█'.repeat(filled) + ' '.repeat(40 - filled);
  process.stdout.write(`\r📊 Processing NDJSON: [${bar}] ${percent.toFixed(1)}% (${current} lines)`);
  if (done) console.log('\n✅ Parsing complete.\n');
}

async function processNDJSON(filePath) {
  const stream = fs.createReadStream(filePath);
  const rl = readline.createInterface({ input: stream });
  const counts = {};
  let lineCount = 0;

  for await (const line of rl) {
    lineCount++;
    if (lineCount % 1000 === 0) renderProgress(lineCount);
    if (!line.trim()) continue;

    try {
      const profile = JSON.parse(line);
      const order = profile?.mostRecentOrder;
      if (order?.title && order?.sku) {
        const key = `${order.title} (SKU: ${order.sku})`;
        counts[key] = (counts[key] || 0) + 1;
      }
    } catch {}
  }

  renderProgress(lineCount, true);
  return { counts, totalProfiles: lineCount };
}

async function createPieChart(counts, totalProfiles) {
  const sorted = Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, MAX_PRODUCTS);

  const labels = sorted.map(([label]) => label);
  const values = sorted.map(([, value]) => value);
  const total = values.reduce((acc, val) => acc + val, 0);
  const percentages = values.map((val) => ((val / total) * 100).toFixed(1));
  const now = new Date().toLocaleString();

  const titleText = [
    'Most Recently Purchased Products',
    `Segment Name: ${SEGMENT_NAME}`,
    `Profiles Enriched: ${totalProfiles.toLocaleString()}`,
    `Generated: ${now}`
  ].join('\n');

  return chartJSNodeCanvas.renderToBuffer({
    type: 'pie',
    data: {
      labels,
      datasets: [{
        label: 'Most Recently Purchased Products',
        data: values,
        backgroundColor: labels.map((_, i) => `hsl(${i * 36}, 70%, 70%)`)
      }]
    },
    options: {
      layout: {
        padding: {
          top: 100,
          bottom: 30,
          left: 80,
          right: 80
        }
      },
      plugins: {
        title: {
          display: true,
          text: titleText,
          color: '#004175',
          font: { size: 18, weight: 'bold' },
          padding: { top: 20, bottom: 20 },
          align: 'center' // Ensuring the title text is centered
        },
        legend: {
          position: 'right',
          labels: {
            color: '#004175',
            font: { weight: 'bold' },
            boxWidth: 20,
            padding: 20,
            generateLabels: (chart) =>
              chart.data.labels.map((label, i) => ({
                text: `${label} (${percentages[i]}%) (${values[i].toLocaleString()})`,
                fillStyle: chart.data.datasets[0].backgroundColor[i],
                strokeStyle: chart.data.datasets[0].backgroundColor[i],
                index: i
              }))
          }
        },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const value = ctx.raw;
              const pct = percentages[ctx.dataIndex];
              return `${ctx.label}: ${value} (${pct}%)`;
            }
          }
        },
        datalabels: {
          color: '#004175',
          formatter: (_, ctx) => `${percentages[ctx.dataIndex]}%`,
          font: { weight: 'bold', size: 14 }
        }
      }
    },
    plugins: [ChartDataLabels]
  }, 'image/png');
}

// Run
(async () => {
  console.log('🚀 Starting NDJSON parsing...');
  const { counts, totalProfiles } = await processNDJSON(FILE_PATH);

  console.log('🎨 Generating pie chart...');
  const image = await createPieChart(counts, totalProfiles);

  fs.writeFileSync(OUTPUT_PATH, image);
  console.log(`✅ Pie chart saved to ${OUTPUT_PATH}`);
})();
