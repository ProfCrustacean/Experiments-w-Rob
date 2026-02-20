import { closePool } from "../db/client.js";
import { runMigrations } from "../db/migrate.js";

async function main(): Promise<void> {
  await runMigrations();
}

main()
  .then(async () => {
    await closePool();
    // eslint-disable-next-line no-console
    console.log("Migrations completed.");
  })
  .catch(async (error: unknown) => {
    // eslint-disable-next-line no-console
    console.error("Migration failed.", error);
    await closePool();
    process.exitCode = 1;
  });
