import { fetchTickersData } from './fetch.js';

async function upsertFundamentalData(symbol, tickerData, database) {
    console.log(`Upserting fundamentals for symbol: ${symbol}`);
    try {
        const query = `
INSERT INTO fundamentals (
    symbol, avg10DaysVolume, avg1YearVolume, declarationDate, divAmount, 
    divExDate, divFreq, divPayAmount, divPayDate, divYield, 
    eps, fundLeverageFactor, lastEarningsDate, nextDivExDate, 
    nextDivPayDate, peRatio, sharesOutstanding, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch())
    ON CONFLICT(symbol) DO UPDATE SET
        avg10DaysVolume = excluded.avg10DaysVolume,
        avg1YearVolume = excluded.avg1YearVolume,
        declarationDate = excluded.declarationDate,
        divAmount = excluded.divAmount,
        divExDate = excluded.divExDate,
        divFreq = excluded.divFreq,
        divPayAmount = excluded.divPayAmount,
        divPayDate = excluded.divPayDate,
        divYield = excluded.divYield,
        eps = excluded.eps,
        fundLeverageFactor = excluded.fundLeverageFactor,
        lastEarningsDate = excluded.lastEarningsDate,
        nextDivExDate = excluded.nextDivExDate,
        nextDivPayDate = excluded.nextDivPayDate,
        peRatio = excluded.peRatio,
        sharesOutstanding = excluded.sharesOutstanding,
        updated_at = unixepoch();
`;

        const values = [
            symbol,
            tickerData.avg10DaysVolume ?? null,
            tickerData.avg1YearVolume ?? null,
            tickerData.declarationDate ?? null,
            tickerData.divAmount ?? null,
            tickerData.divExDate ?? null,
            tickerData.divFreq ?? null,
            tickerData.divPayAmount ?? null,
            tickerData.divPayDate ?? null,
            tickerData.divYield ?? null,
            tickerData.eps ?? null,
            tickerData.fundLeverageFactor ?? null,
            tickerData.lastEarningsDate ?? null,
            tickerData.nextDivExDate ?? null,
            tickerData.nextDivPayDate ?? null,
            tickerData.peRatio ?? null,
            tickerData.sharesOutstanding ?? null
        ];

        const stmt = database.prepare(query);
        await stmt.bind(...values).run();
        console.log(`Successfully updated fundamentals for ${symbol}`);
    } catch (error) {
        console.error(`Error upserting fundamentals for ${symbol}:`, error);
        throw error;
    }
}

async function updateAllTickers(database) {
    console.log('Starting fundamental update for all user tickers');
    try {
        const query = "SELECT layout FROM user_layouts";
        const { results } = await database.prepare(query).all();
        
        const uniqueSymbols = new Set();

        for (const row of results) {
            if (!row.layout) continue;
            try {
                const layout = JSON.parse(row.layout);
                for (const widget of layout) {
                    if (widget.symbol) {
                        uniqueSymbols.add(widget.symbol.toUpperCase());
                    }
                }
            } catch (e) {
                console.error('Error parsing layout JSON:', e);
            }
        }

        const symbolsToFetch = Array.from(uniqueSymbols);
        console.log(`Found ${symbolsToFetch.length} unique symbols:`, symbolsToFetch);

        if (symbolsToFetch.length > 0) {
            // Chunking into groups of 50 to avoid HTTP GET URL length limits
            const chunkSize = 50; 
            for (let i = 0; i < symbolsToFetch.length; i += chunkSize) {
                const chunk = symbolsToFetch.slice(i, i + chunkSize);
                const tickersData = await fetchTickersData(chunk);
                
                for (const [symbol, tickerData] of Object.entries(tickersData)) {
                    await upsertFundamentalData(symbol, tickerData, database);
                }
            }
        }

        console.log('Fundamental update completed!');
    } catch (error) {
        console.error('Error during update:', error);
    }
}

export default {
    async fetch(request, env) {
        return new Response(null, { status: 204 });
    },

    async scheduled(event, env, ctx) {
        console.log("Cron job triggered");
        try {
            await updateAllTickers(env.DB);
            return new Response('Update completed!');
        } catch (error) {
            console.error('Error during update:', error);
            return new Response('Error during update', { status: 500 });
        }
    },
};
