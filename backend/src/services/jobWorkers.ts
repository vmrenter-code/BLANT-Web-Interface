import { Worker, Job } from 'bullmq';
import { JobData, BASIC_BLANT_RUN_K_MIN, BASIC_BLANT_RUN_K_MAX } from '../../types/types';
import IORedis from 'ioredis';
import { spawn } from 'child_process';
import fs from 'fs';
import { getJobFromQueue, updateJobInQueue } from '../config/queue';
import * as path from 'path';
import nodemailer from 'nodemailer';
import archiver from 'archiver';
import os from 'os';

require('dotenv').config();

console.log("Worker started");

const connection = new IORedis({
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT) || 6379,
    maxRetriesPerRequest: null,
    password: process.env.REDIS_PASSWORD,
});

const transporter = nodemailer.createTransport({
    host: process.env.EMAIL_HOST,
    port: 465,
    secure: true,
    auth: {
      user: process.env.EMAIL_USER,
      pass: process.env.EMAIL_PASSWORD,
    },
});

const sendEmail = (recipientEmail: string, subject: string, text: string) => {
  const mailOptions = {
      from: `BLANT Hayes Lab <${process.env.EMAIL_USER}>`,
      to: recipientEmail,
      subject: subject,   
      text: text,
  };
  transporter.sendMail(mailOptions).catch((error) => {
      console.error(`Error sending email:`, error);
  });
}

const blantDirectory = process.env.BLANT_DIRECTORY;
const worker = new Worker('jobQueue', async (job: Job) => {
        console.log("Worker Started Processing Job: ", job.id);
        if (job.data.tool === 'basicBlantRun') {
            await basicBlantRunWorker(job.id, job.data);
        } else if (job.data.tool === 'networkSynthesis') {
            await networkSynthesisWorker(job.id, job.data);
        } else {
            // community detection (also jobs queued before `tool` existed)
            await jobWorker(job.id, job.data);
        }
        console.log("Worker Completed Job: ", job.id);
    },
    {
        connection,
        removeOnComplete: { count: 1000 },
        removeOnFail: { count: 5000 },
        concurrency: 1,
        useWorkerThreads: true,
    },
);

worker.on('failed', (job: Job | undefined, error: Error, prev: string) => {

    console.error(`Job failed:`, { jobId: job?.id, error: error.message, prev });
});

worker.on('completed', (job: Job, returnvalue: any) => {

    console.log(`Job completed:`, { jobId: job.id, returnvalue });
});  

const jobWorker = async (jobId: string, jobData: JobData) => {

        // Construct absolute paths

        const networkDir = path.resolve(`./process/${jobId}`, 'networks', `${jobData.networkName}${jobData.extension}`);

        const outputFile = path.resolve(`./process/${jobId}`, 'blant_runtime.log');

        const optionString = `cd ${blantDirectory} && source ./setup.sh && stdbuf -oL -eL ./scripts/blant-clusters.sh` 
                             + ` -o ${jobData.fractionalOverlap} ./blant ${jobData.graphletSize} ${jobData.density} ${networkDir}`;
        
        console.log(`Executing command for job ${jobId}:`, optionString);
        
        return new Promise((resolve, reject) => {
            const child = spawn('/bin/bash', ['-c', optionString]);
            const logStream = fs.createWriteStream(outputFile, { flags: 'a', autoClose: false });

            let stdout = '';
            // let stderr = '';
            let fileDescriptor: number | null = null;
            let streamReady = true;

            logStream.on('open', (fd: number) => {
                fileDescriptor = fd;
            });

            const writeToStream = (data: Buffer) => {
                if (streamReady) {
                    streamReady = logStream.write(data);
                    if (fileDescriptor) {
                        fs.fdatasync(fileDescriptor, (err) => {
                            if (err) throw err;
                            console.log('Data flushed to disk.');
                
                            // wstream.end(); // Close the stream after flushing
                        });
                    }
                        
                    if (!streamReady) {
                        logStream.once('drain', () => {
                            streamReady = true;
                        });
                    }
                }
            };

            child.stdout.on('data', async (data: Buffer) => {
                const dataStr = data.toString();
                stdout += dataStr;
                // Optional: log in real-time
                console.log(`Job ${jobId} stdout data.toString():`, data.toString());
                console.log(`Job ${jobId} stdout data:`, data);
                // logStream.write(data);
                writeToStream(data);
                await updateJobInQueue(jobId, { execLogFileOutput: stdout });
            });
            
            child.stderr.on('data', async (data: Buffer) => {
                stdout += data.toString();
                console.warn(`Job ${jobId} stderr:`, data.toString());
                logStream.write(data);
                await updateJobInQueue(jobId, { execLogFileOutput: stdout });
            });
            
            child.on('close', async (code) => {
                logStream.end();
                const job = await getJobFromQueue(jobId);
                const email = job?.data.email;
                if (email) {
                    sendEmail(email, `[BLANT Hayes Lab] Job Completed (${jobId})`, `Job completed successfully with code ${code}. View the results here: https://hayeslab.ics.uci.edu/blant/lookup-job/${jobId}`);
                }

                if (code === 0) {
                    console.log(`Job ${jobId} completed successfully with code ${code}`);
                    resolve({ success: true, stdout });
                } else {
                    console.error(`Job ${jobId} failed with code ${code}`);
                    reject(new Error(`Process exited with code ${code}`));
                }
            });
            
            child.on('error', (error) => {
                console.error(`Job ${jobId} error:`, error);
                logStream.end();
                reject(error);
            });
        });

}

// ---------------------------------------------------------------------------
// Basic BLANT Run: ./blant -k <k> network.el
// ---------------------------------------------------------------------------

const BASIC_BLANT_RUN_TIMEOUT_MS = 10 * 60 * 1000;   // kill blant after 10 minutes
const MAX_LIVE_OUTPUT_CHARS = 1_000_000;             // cap on output kept in Redis (full output stays in the log file)
const LIVE_OUTPUT_UPDATE_MS = 1000;                  // push output to Redis at most once a second

const basicBlantRunWorker = async (jobId: string, jobData: JobData) => {
    // Second safety check: never run blant with an unexpected k, even if the queue data was tampered with.
    const k = jobData.k;
    if (!Number.isInteger(k) || k < BASIC_BLANT_RUN_K_MIN || k > BASIC_BLANT_RUN_K_MAX) {
        throw new Error(`Invalid k for job ${jobId}: ${k}`);
    }
    if (!blantDirectory) {
        throw new Error('BLANT_DIRECTORY is not set.');
    }

    // Paths are built only from the server-generated job ID and the name multer accepted.
    const jobDir = path.resolve('./process', jobId);
    const networkPath = path.join(jobDir, 'networks', `${jobData.networkName}${jobData.extension}`);
    if (!networkPath.startsWith(jobDir + path.sep) || !fs.existsSync(networkPath)) {
        throw new Error(`Network file for job ${jobId} not found.`);
    }
    // blant writes its results to stdout and its "Note:" progress messages (and errors) to stderr.
    // Keep them apart so the Lookup page can show the results on their own.
    const progressFile = path.join(jobDir, 'blant_runtime.log');
    const resultsFile = path.join(jobDir, 'results.txt');

    // Arguments are passed as an array: no shell, so nothing in them can be run as a command.
    const args = ['-k', String(k), networkPath];
    console.log(`Basic BLANT Run job ${jobId}: ./blant ${args.join(' ')} (cwd ${blantDirectory})`);

    return new Promise((resolve, reject) => {
        const child = spawn('./blant', args, {
            cwd: blantDirectory,
            // setup.sh is not needed for plain `blant -k` (it only adds helper-script folders to PATH),
            // but BLANT_HOME tells blant where its canon_maps lookup tables are.
            env: { ...process.env, BLANT_HOME: blantDirectory },
            timeout: BASIC_BLANT_RUN_TIMEOUT_MS,
            killSignal: 'SIGKILL',
        });
        const progressStream = fs.createWriteStream(progressFile, { flags: 'a' });
        const resultsStream = fs.createWriteStream(resultsFile, { flags: 'w' });

        // Copies kept in Redis so the Lookup page can show them while the job runs (capped; full copies are in the files).
        let progress = '';
        let results = '';
        let settled = false;
        let pendingUpdate: NodeJS.Timeout | null = null;
        let lastUpdate = 0;

        const keepTail = (text: string) =>
            text.length > MAX_LIVE_OUTPUT_CHARS ? text.slice(-MAX_LIVE_OUTPUT_CHARS) : text;

        const saveToQueue = () =>
            updateJobInQueue(jobId, { execLogFileOutput: progress, resultsOutput: results }).catch((err) =>
                console.error(`Job ${jobId}: could not update output in queue:`, err),
            );

        const scheduleSave = () => {
            if (pendingUpdate) return;
            const wait = Math.max(0, LIVE_OUTPUT_UPDATE_MS - (Date.now() - lastUpdate));
            pendingUpdate = setTimeout(() => {
                pendingUpdate = null;
                lastUpdate = Date.now();
                saveToQueue();
            }, wait);
        };

        const addProgress = (text: string) => {
            progressStream.write(text);
            progress = keepTail(progress + text);
            scheduleSave();
        };

        child.stdout.on('data', (data: Buffer) => {
            resultsStream.write(data);
            results = keepTail(results + data.toString());
            scheduleSave();
        });
        child.stderr.on('data', (data: Buffer) => addProgress(data.toString()));

        const finish = (error: Error | null) => {
            if (settled) return;
            settled = true;
            if (pendingUpdate) clearTimeout(pendingUpdate);
            saveToQueue().finally(() => {
                resultsStream.end(() =>
                    progressStream.end(() => (error ? reject(error) : resolve({ success: true }))),
                );
            });
        };

        child.on('error', (error) => {
            console.error(`Job ${jobId} could not start blant:`, error);
            addProgress(`\nCould not start BLANT: ${error.message}\n`);
            finish(error);
        });

        child.on('close', (code, signal) => {
            if (code === 0) {
                console.log(`Job ${jobId} completed successfully`);
                finish(null);
            } else if (signal === 'SIGKILL') {
                const message = `BLANT run timed out after ${BASIC_BLANT_RUN_TIMEOUT_MS / 60000} minutes.`;
                addProgress(`\n${message}\n`);
                finish(new Error(message));
            } else {
                console.error(`Job ${jobId} failed with code ${code}`);
                addProgress(`\nBLANT stopped with an error (exit code ${code ?? signal}).\n`);
                finish(new Error(`blant exited with code ${code}`));
            }
        });
    });
};

// ---------------------------------------------------------------------------
// Network Synthesis (placeholder phase)
//   Step 1: synthesis        -> placeholder until SynthNetGen is finished
//   Step 2: community detection on the uploaded network (optional)
//   Step 3: zip the uploaded network + its communities for download
// ---------------------------------------------------------------------------

const COMMUNITY_DETECTION_TIMEOUT_MS = 30 * 60 * 1000; // kill blant-clusters.sh after 30 minutes
// blant-clusters.sh can hit a fatal error, or blant can crash inside it, and the script still
// exits 0. So its log is checked for these messages too (macOS and Linux wordings).
const FATAL_ERROR_PATTERN = /fatal error|fatal:|assertion failed|abort trap|aborted|segmentation fault|core dumped/i;

// Writes a zip containing `entries` (absolute source path -> name inside the zip).
const writeZip = (zipPath: string, entries: { source: string; name: string }[]): Promise<void> =>
    new Promise((resolve, reject) => {
        const output = fs.createWriteStream(zipPath);
        const archive = archiver('zip', { zlib: { level: 9 } });
        output.on('close', () => resolve());
        output.on('error', reject);
        archive.on('error', reject);
        archive.pipe(output);
        for (const entry of entries) {
            archive.file(entry.source, { name: entry.name });
        }
        archive.finalize();
    });

// Runs `blant-clusters.sh <args>` in BLANT_DIRECTORY. The communities (stdout) are written to
// `communitiesFile` and passed to onCommunities; progress notes and errors (stderr) go to onProgress.
// Values in `args` are passed as separate arguments ("$@"), never pasted into the command string.
const runBlantClusters = (
    args: string[],
    communitiesFile: string,
    handlers: { onCommunities: (text: string) => void; onProgress: (text: string) => void },
): Promise<void> =>
    new Promise((resolve, reject) => {
        const child = spawn(
            '/bin/bash',
            ['-c', 'source ./setup.sh && exec stdbuf -oL -eL ./scripts/blant-clusters.sh "$@"', 'bash', ...args],
            {
                cwd: blantDirectory,
                env: { ...process.env, BLANT_HOME: blantDirectory },
                timeout: COMMUNITY_DETECTION_TIMEOUT_MS,
                killSignal: 'SIGKILL',
            },
        );
        const communitiesStream = fs.createWriteStream(communitiesFile, { flags: 'w' });
        let sawFatalError = false;
        let stderrTail = ''; // last few characters, so a message split across two chunks is still seen
        let settled = false;
        const finish = (error: Error | null) => {
            if (settled) return;
            settled = true;
            communitiesStream.end(() => (error ? reject(error) : resolve()));
        };

        child.stdout.on('data', (data: Buffer) => {
            communitiesStream.write(data);
            handlers.onCommunities(data.toString());
        });
        child.stderr.on('data', (data: Buffer) => {
            const text = data.toString();
            if (FATAL_ERROR_PATTERN.test(stderrTail + text)) sawFatalError = true;
            stderrTail = text.slice(-32);
            handlers.onProgress(text);
        });

        child.on('error', (error) => {
            handlers.onProgress(`\nCould not start community detection: ${error.message}\n`);
            finish(error);
        });
        child.on('close', (code, signal) => {
            if (code === 0 && !sawFatalError) {
                finish(null);
            } else if (code === 0) {
                handlers.onProgress('\nCommunity detection reported a fatal error (see above).\n');
                finish(new Error('blant-clusters.sh reported a fatal error'));
            } else if (signal === 'SIGKILL') {
                const message = `Community detection timed out after ${COMMUNITY_DETECTION_TIMEOUT_MS / 60000} minutes.`;
                handlers.onProgress(`\n${message}\n`);
                finish(new Error(message));
            } else {
                handlers.onProgress(`\nCommunity detection stopped with an error (exit code ${code ?? signal}).\n`);
                finish(new Error(`blant-clusters.sh exited with code ${code}`));
            }
        });
    });

const networkSynthesisWorker = async (jobId: string, jobData: JobData) => {
    const runCommunityDetection = jobData.runCommunityDetection === true;
    // Second safety check: never run blant-clusters.sh with unexpected options, even if the queue data was tampered with.
    if (runCommunityDetection) {
        const { graphletSize, density, fractionalOverlap } = jobData;
        if (!Number.isInteger(graphletSize) || graphletSize < 3 || graphletSize > 7) {
            throw new Error(`Invalid graphlet size for job ${jobId}: ${graphletSize}`);
        }
        if (typeof density !== 'number' || !(density >= 0.01 && density <= 1)) {
            throw new Error(`Invalid density for job ${jobId}: ${density}`);
        }
        if (typeof fractionalOverlap !== 'number' || !(fractionalOverlap >= 0 && fractionalOverlap < 1)) {
            throw new Error(`Invalid fractional overlap for job ${jobId}: ${fractionalOverlap}`);
        }
        if (!blantDirectory) {
            throw new Error('BLANT_DIRECTORY is not set.');
        }
    }

    // Paths are built only from the server-generated job ID and the name multer accepted.
    const jobDir = path.resolve('./process', jobId);
    const networkFileName = path.basename(`${jobData.networkName}${jobData.extension}`);
    const networkPath = path.join(jobDir, 'networks', networkFileName);
    if (!networkPath.startsWith(jobDir + path.sep) || !fs.existsSync(networkPath)) {
        throw new Error(`Network file for job ${jobId} not found.`);
    }
    const progressFile = path.join(jobDir, 'blant_runtime.log');
    const communitiesFile = path.join(jobDir, 'communities.txt');

    const progressStream = fs.createWriteStream(progressFile, { flags: 'a' });
    let progress = '';
    let communities = '';
    let pendingUpdate: NodeJS.Timeout | null = null;
    let lastUpdate = 0;

    const keepTail = (text: string) =>
        text.length > MAX_LIVE_OUTPUT_CHARS ? text.slice(-MAX_LIVE_OUTPUT_CHARS) : text;

    const saveToQueue = (extra: Partial<JobData> = {}) =>
        updateJobInQueue(jobId, { execLogFileOutput: progress, resultsOutput: communities, ...extra }).catch((err) =>
            console.error(`Job ${jobId}: could not update output in queue:`, err),
        );

    const scheduleSave = () => {
        if (pendingUpdate) return;
        const wait = Math.max(0, LIVE_OUTPUT_UPDATE_MS - (Date.now() - lastUpdate));
        pendingUpdate = setTimeout(() => {
            pendingUpdate = null;
            lastUpdate = Date.now();
            saveToQueue();
        }, wait);
    };

    const addProgress = (text: string) => {
        progressStream.write(text);
        progress = keepTail(progress + text);
        scheduleSave();
    };

    const closeProgress = () => new Promise<void>((resolve) => progressStream.end(() => resolve()));

    try {
        // Step 1: synthesis.
        // TODO(SynthNetGen): once SynthNetGen is finished, run it here, e.g.
        //   rpllb.sh <k:4|5> <input.el> <output/<name>_synth.el> <RSEAS_rounds> <RMSE_rounds> <r>
        // and add the synthetic network to the zip in step 3.
        addProgress('Step 1/3: Network synthesis is not available yet (SynthNetGen is still in development). Using the uploaded network.\n');

        // Step 2: community detection on the uploaded network.
        if (runCommunityDetection) {
            addProgress('Step 2/3: Running community detection...\n');
            // blant-clusters.sh does not quote its network path, so a path with spaces breaks it
            // (e.g. a local checkout under "Hayes Website"). Give it a copy at a path without spaces.
            let clustersNetworkPath = networkPath;
            let tempNetworkDir: string | null = null;
            if (/\s/.test(networkPath)) {
                tempNetworkDir = fs.mkdtempSync(path.join(os.tmpdir(), 'network-synthesis-'));
                clustersNetworkPath = path.join(tempNetworkDir, networkFileName);
                fs.copyFileSync(networkPath, clustersNetworkPath);
            }
            // Values are passed as separate arguments ("$@"), never pasted into the command string.
            const args = [
                '-o', String(jobData.fractionalOverlap),
                './blant',
                String(jobData.graphletSize),
                String(jobData.density),
                clustersNetworkPath,
            ];
            console.log(`Network Synthesis job ${jobId}: blant-clusters.sh ${args.join(' ')} (cwd ${blantDirectory})`);

            try {
                await runBlantClusters(args, communitiesFile, {
                    onCommunities: (text) => {
                        communities = keepTail(communities + text);
                        scheduleSave();
                    },
                    onProgress: addProgress,
                });
            } finally {
                if (tempNetworkDir) fs.rmSync(tempNetworkDir, { recursive: true, force: true });
            }
        } else {
            addProgress('Step 2/3: Community detection was not requested; skipping.\n');
        }

        // Step 3: zip the uploaded network and (if run) its communities.
        addProgress('Step 3/3: Creating the results zip...\n');
        const zipName = `${jobData.networkName}_results.zip`;
        const entries = [{ source: networkPath, name: networkFileName }];
        if (runCommunityDetection) {
            entries.push({ source: communitiesFile, name: `${jobData.networkName}_communities.txt` });
        }
        await writeZip(path.join(jobDir, path.basename(zipName)), entries);

        const infoFile = path.join(jobDir, 'info.json');
        fs.writeFileSync(
            infoFile,
            JSON.stringify({ status: 'processed', zipName, data: { ...jobData, zipName } }, null, 2),
        );
        addProgress('Done. Your results zip is ready to download.\n');

        if (pendingUpdate) clearTimeout(pendingUpdate);
        await saveToQueue({ zipName });
        console.log(`Network Synthesis job ${jobId} completed`);
    } catch (error) {
        if (pendingUpdate) clearTimeout(pendingUpdate);
        await saveToQueue();
        throw error;
    } finally {
        await closeProgress();
    }
};

export { worker, jobWorker, basicBlantRunWorker, networkSynthesisWorker };