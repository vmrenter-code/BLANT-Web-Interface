// src/components/NetworkSynthesisOutput/NetworkSynthesisOutput.tsx
// Shows a Network Synthesis job on the Lookup page: status, a Download button for the results zip,
// a preview of the communities found, and the progress log. Styled like the Basic BLANT Run output.
import React, { useState } from 'react';
import '../BasicBlantRunOutput/BasicBlantRunOutput.css';
import '../Options/Options.css';
import api from '../../api/api';

export type NetworkSynthesisState = 'running' | 'done' | 'failed';

interface NetworkSynthesisOutputProps {
  jobId: string;
  state: NetworkSynthesisState;
  communities: string;
  progress: string;
  zipReady: boolean;
  failureMessage?: string | null;
  onCancel?: () => void;
}

const NetworkSynthesisOutput: React.FC<NetworkSynthesisOutputProps> = ({
  jobId, state, communities, progress, zipReady, failureMessage, onCancel,
}) => {
  const [isDownloading, setIsDownloading] = useState(false);

  const handleDownload = async () => {
    setIsDownloading(true);
    try {
      const blob = await api.downloadJobZip(jobId);
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `${jobId}_results.zip`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    } catch (error) {
      alert(`Could not download the results zip. ${error instanceof Error ? error.message : ''}`);
    }
    setIsDownloading(false);
  };

  return (
    <div className="bbr-container" role="region" aria-label="Network Synthesis job output">
      <h2 className="bbr-title">Graphlet-Based Network Synthesis</h2>
      <p className="bbr-jobId">Job ID: <strong>{jobId}</strong></p>

      {state === 'running' && (
        <div className="bbr-status" role="status" aria-live="polite">
          Your job is running… the download button will appear here when it finishes.
          {onCancel && (
            <button onClick={onCancel} className="bbr-cancel" aria-label="Cancel current job">Cancel Job</button>
          )}
        </div>
      )}
      {state === 'failed' && (
        <div className="bbr-status bbr-status-failed" role="alert">
          {failureMessage || 'The job stopped with an error. See the progress log below.'}
        </div>
      )}
      {state === 'done' && zipReady && (
        <div className="bbr-status" role="status">
          Your results are ready.
          <button
            onClick={handleDownload}
            className="os-navButton"
            disabled={isDownloading}
            aria-label="Download results zip file"
          >
            {isDownloading ? 'Downloading…' : 'Download results (.zip)'}
          </button>
        </div>
      )}

      <section className="bbr-section" aria-labelledby="ns-communities-heading">
        <div className="bbr-sectionHeader">
          <h3 id="ns-communities-heading">Communities</h3>
        </div>
        {communities.trim() ? (
          <pre className="bbr-pre" aria-label="Communities found">{communities}</pre>
        ) : (
          <p className="bbr-empty">
            {state === 'running'
              ? 'Waiting for community detection…'
              : 'No communities to show (community detection was not run, or it found none).'}
          </p>
        )}
      </section>

      <details className="bbr-section bbr-progress" open={state !== 'done'}>
        <summary>Progress log {progress ? `(${progress.trim().split('\n').length} lines)` : ''}</summary>
        <pre className="bbr-pre" aria-label="Progress log">{progress || 'No progress messages yet.'}</pre>
      </details>
    </div>
  );
};

export default NetworkSynthesisOutput;
