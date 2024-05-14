import { HypersyncClient, Decoder } from "@envio-dev/hypersync-client";
import fs from "node:fs";

const hypersyncRpcUrl: string = "https://eth.hypersync.xyz";

// The token addresses we want to get data for
const tokenAddresses: string[] = [
    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC
];

// The event hashes we want to get data for
const eventHashes: string[] = [
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", // Transfer
];

// The addresses we want to get data for
const contractAddresses: string[] = [
    "0x48c04ed5691981C42154C6167398f95e8f38a7fF",
];

function convertAddressesToTopics(addresses: string[]): string[] {
    return addresses.map(
        (addr) => "0x000000000000000000000000" + addr.slice(2, addr.length)
    );
}

function lowerCaseAddresses(addresses: string[]): string[] {
    return addresses.map((addr) => addr.toLowerCase());
}

async function main() {
    // Create hypersync client using the mainnet hypersync endpoint
    const client = HypersyncClient.new({
        url: hypersyncRpcUrl,
    });

    const lowerCaseContractAddresses = lowerCaseAddresses(contractAddresses);
    const contractAddressTopics = convertAddressesToTopics(
        lowerCaseContractAddresses
    );

    const query = {
        // Start from block 0 and go to the end of the chain (we don't specify a toBlock).
        // @todo add iteration
        fromBlock: 15000000,
        // The logs we want. We will also automatically get transactions and blocks relating to these logs (the query implicitly joins them).
        logs: [
            {
                address: tokenAddresses,
                topics: [eventHashes, [], contractAddressTopics],
            },
            {
                address: tokenAddresses,
                topics: [eventHashes, contractAddressTopics, []],
            },
        ],
        // Get all transactions coming from and going to any of our addresses.
        transactions: [
            {
                from: lowerCaseContractAddresses,
            },
            {
                to: lowerCaseContractAddresses,
            },
        ],
        // Select the fields we are interested in, notice topics are selected as topic0,1,2,3
        fieldSelection: {
            log: [
                "block_number",
                "log_index",
                "transaction_index",
                "transaction_hash",
                "data",
                "address",
                "topic0",
                "topic1",
                "topic2",
            ],
            transaction: [
                "block_number",
                "transaction_index",
                "hash",
                "from",
                "to",
                "value",
                "input",
            ],
        },
    };

    console.log("Running the query...");

    // Run the query once, the query is automatically paginated so it will return when it reaches some limit (time, response size etc.)
    // there is a nextBlock field on the response object so we can set the fromBlock of our query to this value and continue our query until
    // res.nextBlock is equal to res.archiveHeight or query.toBlock in case we specified an end block.
    const res = await client.sendReq(query);

    console.log(`Ran the query once. Next block to query is ${res.nextBlock}`);

    // Read json abi file for erc20
    const abi = fs.readFileSync("./erc20.abi.json", "utf8");
    const parsedAbi = JSON.parse(abi);

    // Map from contract address to ABI
    const abis = {};

    // Every log we get should be decodable, so we need to store the ABI for each contract address.
    for (const tokenAddress of tokenAddresses) {
        abis[tokenAddress] = parsedAbi;
    }

    // Create a decoder with our mapping
    const decoder = Decoder.new(abis);

    // Decode the log on a background thread so we don't block the event loop.
    // Can also use decoder.decodeLogsSync if it is more convenient.
    const decodedLogs = await decoder.decodeLogs(res.data.logs);

    // @question Do we need the volume or something else?
    // @todo should split for different tokens

    const totalTokensVolume = {};

    for (const log of decodedLogs) {
        const tokensAmount: bigint = log.body[0].val as bigint;

        if (tokensAmount === BigInt(0)) {
            continue;
        }

        const fromAddress: string = log.indexed[0].val as string;
        const toAddress: string = log.indexed[1].val as string;

        if (!totalTokensVolume[fromAddress]) {
            totalTokensVolume[fromAddress] = BigInt(0);
        }
        if (!totalTokensVolume[toAddress]) {
            totalTokensVolume[toAddress] = BigInt(0);
        }

        totalTokensVolume[fromAddress] += tokensAmount;
        totalTokensVolume[toAddress] += tokensAmount;
    }

    for (const addr of lowerCaseContractAddresses) {
        console.log(
            `ERC20 transfer volume for address ${addr} is ${totalTokensVolume[addr]}`
        );
    }

    const totalWeiVolume = {};

    for (const tx of res.data.transactions) {
        const ethValue: bigint = BigInt(tx.value);

        if (ethValue === BigInt(0)) {
            continue;
        }

        const fromAddress: string = tx.from;
        const toAddress: string = tx.to;

        if (!totalWeiVolume[fromAddress]) {
            totalWeiVolume[fromAddress] = BigInt(0);
        }
        if (!totalWeiVolume[toAddress]) {
            totalWeiVolume[toAddress] = BigInt(0);
        }

        totalWeiVolume[fromAddress] += ethValue;
        totalWeiVolume[toAddress] += ethValue;
    }

    for (const addr of lowerCaseContractAddresses) {
        console.log(
            `WEI transfer volume for address ${addr} is ${totalWeiVolume[addr]}`
        );
    }
}

main();
