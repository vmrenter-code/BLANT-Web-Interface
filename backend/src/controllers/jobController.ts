import fs from 'fs';
import { Response, NextFunction } from 'express';
import HttpError from '../middlewares/HttpError';
import { createJob, createBasicBlantRunJob, createNetworkSynthesisJob } from '../services/jobService';
import { cleanupFile } from '../middlewares/upload';
import { jobProcess } from '../services/processService';
import {
    DownloadZipRequest,
    GetJobResultsRequest,
    GetJobStatusRequest,
    CancelJobRequest,
    CancelJobResponse,
    JobData,
    JobStatusResponse,
    ProcessedJobResponse,
    ProcessJobData,
    SubmitJobOptions,
    SubmitJobRequest,
    SubmitBasicBlantRunRequest,
    SubmitNetworkSynthesisRequest,
    UnifiedResponse,
    BASIC_BLANT_RUN_K_MIN,
    BASIC_BLANT_RUN_K_MAX,
} from '../../types/types';
import path from 'path';
import { getJobFromQueue, getWaitingCount } from '../config/queue';
/**
 * Downloads the zip file for a job based on the request parameters.
 * @param req - The request object.
 * @param res - The response object.
 * @param next - The next middleware function.
 * @returns A Promise that resolves when the download is complete or rejects with an error.
 */
const downloadZipJob = async (req: DownloadZipRequest, res: Response, next: NextFunction): Promise<void> => {
    try {
        const jobId = req.params.id;
        // Job IDs are 32-character md5 hex strings made by the server; anything else could point outside ./process.
        if (!jobId || !/^[a-f0-9]{32}$/.test(jobId)) {
            throw HttpError.badRequest('Invalid job ID.');
        }

        // Same folder the workers write to.
        const jobDir = path.resolve('./process', jobId);
        if (!fs.existsSync(jobDir) || !fs.lstatSync(jobDir).isDirectory()) {
            throw new HttpError('Job not found.', { status: 404 });
        }

        // The worker records the zip name in the queue and in info.json once the zip is written.
        const job = await getJobFromQueue(jobId);
        let zipName: string | undefined = job?.data.zipName;
        if (!zipName) {
            try {
                const info = JSON.parse(fs.readFileSync(path.join(jobDir, 'info.json'), 'utf8'));
                zipName = info.zipName;
            } catch (err) {
                console.error(`Could not read info.json for job ${jobId}:`, err);
            }
        }
        if (!zipName) {
            throw new HttpError('The results zip is not ready yet. Please wait for the job to finish.', { status: 404 });
        }

        const zipLocation = path.join(jobDir, path.basename(zipName));
        if (!fs.existsSync(zipLocation)) {
            throw new HttpError('Zip file not found.', { status: 404 });
        }

        res.download(zipLocation, path.basename(zipName), (err) => {
            if (err) {
                // headers may already be sent; just log and let the download fail
                console.error('Error sending zip file:', err);
            }
        });
    } catch (err) {
        next(err);
    }
};

const _isValidEmail = (email: string): boolean => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
}


const _validateCommunityDetectionOptions = (jobOptions: SubmitJobOptions): void => {
    if (!jobOptions || typeof jobOptions !== 'object') {
        throw HttpError.badRequest('Community detection options are required.');
    }
    if (!jobOptions.density) {
        throw HttpError.badRequest('density is required.');
    }

    if (isNaN(jobOptions.density)) {
        throw HttpError.badRequest('density must be a valid number.');
    } else if (jobOptions.density > 1 || jobOptions.density < 0.01) {
        throw HttpError.badRequest('density must be between 0.01 and 1.00.');
    }

    if (!jobOptions.fractionalOverlap) {
        throw HttpError.badRequest('fractionalOverlap is required.');
    }
    if (isNaN(jobOptions.fractionalOverlap)) {
        throw HttpError.badRequest('fractionalOverlap must be a valid number.');
    } else if (jobOptions.fractionalOverlap >= 1 || jobOptions.fractionalOverlap < 0) {
        throw HttpError.badRequest('fractionalOverlap must be greater than or equal to 0 and less than 1.');
    }

    if (!jobOptions.graphletSize ) {
        throw HttpError.badRequest('graphletSize is required.');
    }
    if (isNaN(jobOptions.graphletSize)) {
        throw HttpError.badRequest('graphletSize must be a valid number.');
    } else if (jobOptions.graphletSize > 7 || jobOptions.graphletSize < 3) {
        throw HttpError.badRequest('graphletSize must be between 3 and 7.');
    }
};

const _validateSubmitJob = (jobOptions: SubmitJobOptions, req: SubmitJobRequest): void => {
    _validateCommunityDetectionOptions(jobOptions);

    if (req.body.email && !_isValidEmail(req.body.email)) {
        throw HttpError.badRequest('Invalid email address.');
    }
    if (!req.file) {
        throw HttpError.badRequest('No network file uploaded.');
    }
}
/**
 * Controller to handle job submission with file uploads.
 */
const submitJobController = async (req: SubmitJobRequest, res: Response, next: NextFunction): Promise<void> => {
    try {
        //  Validate required fields
        console.log("submitJobController req.body:", req.body);
        const jobOptions: SubmitJobOptions = JSON.parse(req.body.options); // parse req.body string into an object
        console.log("submitJobController jobOptions object:", jobOptions);
        const email = req.body.email;

        _validateSubmitJob(jobOptions, req);

        // creates job and runs preprocessing (creating the directory for output files, moving the network files there, etc... but does not actually start running the job)
        // const result = await createJob(req.file, req.body.options.density, req.body.options.graphletSize);
        const result = await createJob(req.file, jobOptions.density, jobOptions.graphletSize, jobOptions.fractionalOverlap, email);
        console.log("cratead job with preprocess data:", result);
        //  Send successful response
        const processResult = await processController(result);
        res.status(201).json(processResult);
    } catch (err) {
        next(err);
    }
};
 
/**
 * Reads a text file from a job's folder, or returns null if it doesn't exist or can't be read.
 * `name` is always a fixed file name chosen by the server, never user input.
 */
const _readJobFile = (jobDir: string, name: string): string | null => {
    const filePath = path.join(jobDir, name);
    try {
        return fs.existsSync(filePath) ? fs.readFileSync(filePath, 'utf8') : null;
    } catch (err) {
        console.error(`Error reading ${filePath}:`, err);
        return null;
    }
};

/**
 * Parses and validates `k` for a Basic BLANT Run.
 * Accepts only a plain whole number (e.g. "4") inside the allowed range.
 */
const _parseBasicBlantRunK = (rawK: unknown): number => {
    if (typeof rawK !== 'string' || !/^\d+$/.test(rawK.trim())) {
        throw HttpError.badRequest('k must be a whole number.');
    }
    const k = Number(rawK.trim());
    if (k < BASIC_BLANT_RUN_K_MIN || k > BASIC_BLANT_RUN_K_MAX) {
        throw HttpError.badRequest(`k must be between ${BASIC_BLANT_RUN_K_MIN} and ${BASIC_BLANT_RUN_K_MAX}.`);
    }
    return k;
};

/**
 * Controller for the Basic BLANT Run tool: `./blant -k <k> network.el`.
 * Expects multipart form data with `file` (.el network) and `k`.
 */
const submitBasicBlantRunController = async (req: SubmitBasicBlantRunRequest, res: Response, next: NextFunction): Promise<void> => {
    try {
        if (!req.file) {
            throw HttpError.badRequest('No network file uploaded.');
        }
        const k = _parseBasicBlantRunK(req.body.k);
        const jobData = await createBasicBlantRunJob(req.file, k);
        const processResult = await processController(jobData);
        res.status(201).json(processResult);
    } catch (err) {
        // On a validation error the upload is still in tmp/; remove it.
        // (After a successful preprocess it has already been moved, and unlink just logs.)
        if (err instanceof HttpError && err.statusCode === 400) {
            cleanupFile(req.file);
        }
        next(err);
    }
};

/**
 * Controller for the Network Synthesis tool.
 * Expects multipart form data with `file` (.el network), `runCommunityDetection` ("true"/"false")
 * and, when community detection is on, `options` (JSON: density, graphletSize, fractionalOverlap).
 */
const submitNetworkSynthesisController = async (req: SubmitNetworkSynthesisRequest, res: Response, next: NextFunction): Promise<void> => {
    try {
        if (!req.file) {
            throw HttpError.badRequest('No network file uploaded.');
        }
        const runCommunityDetection = req.body.runCommunityDetection === 'true';
        let communityDetection: { density: number; graphletSize: number; fractionalOverlap: number } | null = null;
        if (runCommunityDetection) {
            let raw: any;
            try {
                raw = JSON.parse(req.body.options ?? '');
            } catch {
                throw HttpError.badRequest('Community detection options must be valid JSON.');
            }
            const options: SubmitJobOptions = {
                density: Number(raw?.density),
                graphletSize: Number(raw?.graphletSize),
                fractionalOverlap: Number(raw?.fractionalOverlap),
            };
            _validateCommunityDetectionOptions(options);
            if (!Number.isInteger(options.graphletSize)) {
                throw HttpError.badRequest('graphletSize must be a whole number.');
            }
            communityDetection = options;
        }
        const jobData = await createNetworkSynthesisJob(req.file, communityDetection);
        const processResult = await processController(jobData);
        res.status(201).json(processResult);
    } catch (err) {
        // On a validation error the upload is still in tmp/; remove it.
        if (err instanceof HttpError && err.statusCode === 400) {
            cleanupFile(req.file);
        }
        next(err);
    }
};

// Controller to process a job after submission.
const processController = async (data: JobData): Promise<ProcessJobData> => {
    const jobId = data.id;
    const jobData = data;
    if (!jobId) {
        throw new HttpError('id field is required in request.body', { status: 400 });
    }
    return await jobProcess(jobId, jobData);
};

/**
 * Controller function to retrieve job results based on a job ID.
 *
 * @param req - Express request object containing the job ID in `params`.
 * @param res - Express response object that sends back a JSON response.
 * @param next - Express next middleware function for error handling.
 * @returns A Promise that resolves when the response is sent.
 */
const getJobStatus = async (req: GetJobResultsRequest, res: Response, next: NextFunction): Promise<void> => {
    try {
        const jobId = req.params.id;
        console.log('getJobStatus jobId:', jobId);

        if (!jobId) {
            throw new HttpError('Job ID is required.', { status: 400 });
        }

        const job = await getJobFromQueue(jobId);

        if (!job) {
            throw new HttpError(`Job with id ${jobId} does not exist.`, { status: 400 });
        }

        console.log("getJobStatus job: ", job);
        console.log("getJobStatus job.data: ", job?.data ?? 'no data');

        const jobDir = path.resolve("./process", jobId);
        console.log("process.cwd():", process.cwd());
        console.log("__dirname:", __dirname);
        console.log("jobDir:", jobDir);

        const status = await job.getState();
        console.log("getJobStatus status: ", status);

        const isBasicBlantRun = job.data.tool === 'basicBlantRun';
        // Jobs run by an older worker wrote results and progress into one log and have no results.txt;
        // for those, leave out `tool` so the Lookup page shows the single combined log as before.
        const separatedResults = isBasicBlantRun ? _readJobFile(jobDir, 'results.txt') : null;

        // Basic BLANT Run failures: send the progress log (it holds blant's error message)
        // instead of the generic community detection error.
        if (status === 'failed' && isBasicBlantRun) {
            const message = 'BLANT stopped with an error. See the progress log below.';
            res.status(400).json({
                status: 'error',
                message,
                error: { message },
                tool: 'basicBlantRun',
                execLogFileOutput: _readJobFile(jobDir, 'blant_runtime.log') ?? job.data.execLogFileOutput,
                resultsOutput: separatedResults ?? '',
            });
            return;
        }

        // Network Synthesis: progress log, communities preview and whether the zip is ready.
        if (job.data.tool === 'networkSynthesis') {
            const progressLog = _readJobFile(jobDir, 'blant_runtime.log') ?? job.data.execLogFileOutput ?? '';
            const communitiesOutput = _readJobFile(jobDir, 'communities.txt') ?? job.data.resultsOutput ?? '';
            if (status === 'failed') {
                const message = 'The job stopped with an error. See the progress log below.';
                res.status(400).json({
                    status: 'error',
                    message,
                    error: { message },
                    tool: 'networkSynthesis',
                    execLogFileOutput: progressLog,
                    resultsOutput: communitiesOutput,
                    zipReady: false,
                });
                return;
            }
            res.status(200).json({
                status: status === 'completed' ? 'success' : status === 'waiting' ? 'waiting' : 'processing',
                message: status === 'completed' ? 'Job Results' : `Job Status is ${status}.`,
                redirect: `/lookup-job/${jobId}`,
                tool: 'networkSynthesis',
                execLogFileOutput: progressLog,
                resultsOutput: communitiesOutput,
                zipReady: status === 'completed' && Boolean(job.data.zipName),
            });
            return;
        }

        if (status === 'failed') {
            // Read the blant_runtime.log file for error details
            const runLogPath = path.join(jobDir, 'blant_runtime.log');
            let runLogContent = '';

            try {
                if (fs.existsSync(runLogPath)) {
                    runLogContent = fs.readFileSync(runLogPath, 'utf8');
                    runLogContent = runLogContent
                        .split('\n')
                        .map((line) => `<span>${line.trim()}</span>`)
                        .join('\n');
                } else {
                    runLogContent = `Run log file at ${runLogPath} not found`;
                }
            } catch (err) {
                console.error('Error reading blant_runtime.log:', err);
                runLogContent = 'Error reading run log file';
            }

            throw new HttpError('The alignment of the networks failed. See execution log below:', {
                status: 400,
                errorLog: runLogContent,
            });
        }

        if (status === 'completed') {

            // Get execution log
            const execLogFilePath = path.join(jobDir, 'blant_runtime.log');
            let execLogFileOutput = '';

            if (fs.existsSync(execLogFilePath)) {
                try {
                    const execLogFileContent = fs.readFileSync(execLogFilePath, 'utf8');
                    execLogFileOutput = execLogFileContent;
                } catch (err) {
                    execLogFileOutput = 'Problem opening execution log file.';
                }
            } else {
                execLogFileOutput = `Job execution log file at ${execLogFilePath} does not exist.`;
            }

            // Construct base URL for download link
            const baseUrl = `${req.protocol}://${req.get('host')}`;

            const response: UnifiedResponse<ProcessedJobResponse> = {
                status: 'success',
                message: 'Job Results',
                data: {
                    jobId: jobId,
                    note: `These results can be accessed on the results page using the Job ID ${jobId}, 
                        or directly accessed using ${baseUrl}/results?id=${jobId}.`,
                    zipDownloadUrl: `${baseUrl}/api/download/${jobId}`,
                    execLogFileOutput: execLogFileOutput,
                    ...(separatedResults !== null && {
                        tool: 'basicBlantRun' as const,
                        resultsOutput: separatedResults,
                    }),
                },
            };

            res.status(200).json(response);
            return;
        } 

        if (status === 'waiting'){
            const waitingResponse: UnifiedResponse = {
                status: 'waiting',
                message: `There are currently ${getWaitingCount()} jobs waiting in the queue.`,
                redirect: `/lookup-job/${jobId}`,
                execLogFileOutput: job.data.execLogFileOutput,
            };
            res.status(200).json(waitingResponse);
        }

        const execLogFilePath = path.join(job.data.jobLocation, 'blant_runtime.log');
        let execLogContent = '';
        try {
            if (fs.existsSync(execLogFilePath)) {
                execLogContent = fs.readFileSync(execLogFilePath, 'utf8');
            } else {
                execLogContent = `Job execution log file at ${execLogFilePath} does not exist.`;
            }
        } catch (err) {
            execLogContent = 'Error reading execution log file.';
        }
        console.log("jobId: ", jobId, " execLogContent:", execLogContent);

        const redirectResponse: UnifiedResponse = {
            status: 'processing',
            message: `Job Status is ${status}.`,
            redirect: `/lookup-job/${jobId}`,
            execLogFileOutput: job.data.execLogFileOutput,
            ...(isBasicBlantRun && {
                tool: 'basicBlantRun' as const,
                resultsOutput: job.data.resultsOutput ?? '',
            }),
        };
        res.status(200).json(redirectResponse);
    } catch (err) {
        next(err);
    }
};

const cancelJob = async (req: CancelJobRequest, res: Response, next: NextFunction): Promise<void> => {
    try {
        const jobId = req.params.id;
        console.log('cancelJob jobId:', jobId);

        if (!jobId) {
            throw new HttpError('Job ID is required.', { status: 400 });
        }

        const job = await getJobFromQueue(jobId);

        if (!job) {
            throw new HttpError(`Job with id ${jobId} does not exist.`, { status: 400 });
        }
        const jobLocation = job.data.jobLocation;
        fs.rmSync(jobLocation, { recursive: true, force: true });

        const jobState = await job.getState();
        if (jobState === 'active') {
            // Job is locked by worker - mark as failed
            await job.moveToFailed(new Error('Job cancelled by user'), '0', true);
            console.log(`Job ${jobId} marked as failed (was active)`);
        } else {
            // Job is not active - can safely remove
            await job.remove();
            console.log(`Job ${jobId} removed from queue`);
        }

        const response: UnifiedResponse = {
            status: 'success',
            message: 'Job cancelled successfully',
        };

        res.status(200).json(response);
        return;

    } catch (err) {
        console.error('Error cancelling job:', err);
        const response: UnifiedResponse = {
            status: 'error',
            message: 'Error cancelling job: ' + err,
        };
        res.status(500).json(response);
        return;
    }
};


export { 
    downloadZipJob, 
    getJobStatus,
    submitJobController, 
    submitBasicBlantRunController,
    submitNetworkSynthesisController,
    processController,
    cancelJob,
};
