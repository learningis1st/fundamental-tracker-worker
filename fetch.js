export async function fetchTickersData(symbols) {
    if (!symbols || symbols.length === 0) return {};

    const symbolString = symbols.join(',');
    console.log(`Fetching fundamentals for symbols: ${symbolString}`);
    
    try {
        const url = new URL('https://finance.learningis1.st/quote');
        url.searchParams.append('symbol', symbolString);
        url.searchParams.append('fields', 'fundamental');

        const response = await fetch(url.toString());

        if (!response.ok) {
            console.error(`Fetch failed for ${symbolString}. Status: ${response.status}`);
            return {};
        }

        const data = await response.json();
        const validData = {};
        
        for (const [symbol, assetData] of Object.entries(data)) {
            if (!assetData) {
                console.error(`No data returned for ${symbol}`);
                continue;
            }

            if (assetData.assetMainType !== 'EQUITY') {
                console.log(`Skipping fundamentals for ${symbol}: assetMainType is ${assetData.assetMainType}`);
                continue;
            }

            if (!assetData.fundamental) {
                console.error(`No fundamental data found for ${symbol} despite being an EQUITY`);
                continue;
            }

            validData[symbol] = assetData.fundamental;
        }

        return validData;
        
    } catch (error) {
        console.error(`Error fetching data for symbols:`, error);
        return {};
    }
}
