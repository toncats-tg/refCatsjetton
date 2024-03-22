import { getHttpV4Endpoint } from "@orbs-network/ton-access";
import { TonClient4 } from "ton";
import { jettons } from "../data/jettons";
import { connectToDB } from "../engine/db";
import { indexBlockJettonWallets } from "../engine/indexBlockJettonWallets";
import { sleep } from "../utils/sleep";
import { Block } from "../utils/types";

require("dotenv").config();

async function startRealTimeIndexer() {
  await connectToDB();

  const endpoint = await getHttpV4Endpoint();
  const client = new TonClient4({
    endpoint,
  });
  const latestBlock = await client.getLastBlock();

  await realTimeIndexBlock(client, latestBlock.last.seqno);
}

async function realTimeIndexBlock(
  client: TonClient4,
  latestBlockSeqno: number
) {
  const block = await client.getBlock(latestBlockSeqno).catch(async (e) => {
    console.log(
      "[realTimeIndexer.ts] Failed to get block " +
        latestBlockSeqno +
        ". Retrying in 2 seconds..."
    );

    await sleep(2000);
    await realTimeIndexBlock(client, latestBlockSeqno);
  });

  console.log(
    "[realTimeIndexer.ts] Started to work on block " + latestBlockSeqno + "..."
  );

  await indexBlockJettonWallets(
    client,
    block as Block,
    latestBlockSeqno,
    jettons
  );

  await realTimeIndexBlock(client, latestBlockSeqno + 1);
}

// Notes:
// 1. Realtime indexer run on all jettons, so we dont need to specify jetton
// 2. After adding new jetton we have to restart realtime indexer and run historical indexer for this jetton

console.log(
  `[realTimeIndexer.ts] Started realtime indexer for ${jettons
    .map((j) => j.slug)
    .join(", ")}`
);

startRealTimeIndexer();
