// src/pages/NetworkSynthesis/NetworkSynthesis.tsx
// Graphlet-Based Network Synthesis (SynthNetGen).
// SynthNetGen is still being finished, so for now this uploads a network, optionally runs
// community detection on it, and returns one zip (the network + its communities) on the Lookup page.
import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import '../SubmitJob/SubmitJob.css';
import '../../components/Options/Options.css';
import NetworkSelection from '../../components/NetworkSelection';
import Options from '../../components/Options';
import Processing from '../../components/Processing';
import api from '../../api/api';
import type { blantOptions } from '../../types/types';

const MAX_FILE_BYTES = 5 * 1024 * 1024; // matches the backend upload limit

// Same defaults as the Community Detection tool.
const DEFAULT_OPTIONS: blantOptions = {
  graphletSize: 4,
  density: 1,
  fractionalOverlap: 0.5,
};

const validateFile = (file: File): string | null => {
  if (!file.name.toLowerCase().endsWith('.el')) {
    return '[INVALID FILE] File extension must be .el';
  }
  if (/\s/.test(file.name)) {
    return '[INVALID FILE] File names cannot contain spaces';
  }
  if (file.size > MAX_FILE_BYTES) {
    return '[INVALID FILE] File size must not exceed 5 MB';
  }
  return null;
};

// Same ranges the server checks.
const validateOptions = (options: blantOptions): string | null => {
  if (!Number.isInteger(options.graphletSize) || options.graphletSize < 3 || options.graphletSize > 7) {
    return 'Graphlet size must be between 3 and 7.';
  }
  if (!(options.density >= 0.01 && options.density <= 1)) {
    return 'Edge density must be between 0.01 and 1.00.';
  }
  if (!(options.fractionalOverlap >= 0 && options.fractionalOverlap < 1)) {
    return 'Fractional overlap must be at least 0 and less than 1.';
  }
  return null;
};

const NetworkSynthesis: React.FC = () => {
  const navigate = useNavigate();
  const [networkFile, setNetworkFile] = useState<File | null>(null);
  const [runCommunityDetection, setRunCommunityDetection] = useState<boolean>(true);
  const [options, setOptions] = useState<blantOptions>(DEFAULT_OPTIONS);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);

  const handleFileInputChange = async (event: React.ChangeEvent<HTMLInputElement>): Promise<boolean> => {
    const file = event.target.files?.[0];
    if (!file) return false;
    const error = validateFile(file);
    if (error) {
      alert(error);
      event.target.value = '';
      setNetworkFile(null);
      return false;
    }
    setNetworkFile(file);
    return true;
  };

  const handleOptionChange = (
    event: React.ChangeEvent<HTMLSelectElement | HTMLInputElement>,
    optionName: keyof blantOptions,
  ) => {
    const value = Number(event.target.value);
    setOptions((prev) => ({ ...prev, [optionName]: value }));
  };

  const handleSubmit = async () => {
    if (!networkFile) {
      alert('Please upload a network file.');
      return;
    }
    if (runCommunityDetection) {
      const optionsError = validateOptions(options);
      if (optionsError) {
        alert(optionsError);
        return;
      }
    }
    setIsSubmitting(true);
    try {
      const formData = new FormData();
      formData.append('runCommunityDetection', String(runCommunityDetection));
      if (runCommunityDetection) {
        formData.append('options', JSON.stringify(options));
      }
      formData.append('file', networkFile);
      const response = await api.submitNetworkSynthesis(formData);
      if (response.redirect) {
        navigate(response.redirect); // Lookup page for this job
        return;
      }
      alert(response.error?.message || response.message || response.status || 'Job submission failed.');
    } catch (error) {
      alert(String(error));
    }
    setIsSubmitting(false);
  };

  return (
    <div className="sjp-submitJobPage">
      <h1 className="sjp-pageTitle">Graphlet-Based Network Synthesis</h1>

      {isSubmitting ? (
        <div className="sjp-accordion" role="status" aria-live="polite" aria-atomic="true">
          <Processing />
        </div>
      ) : (
        <div className="sjp-accordion" role="region" aria-label="Network Synthesis form">
          <div className="os-stepContainer">
            <p>
              Upload a network and SynthNetGen will generate a new network with the same nodes and edges
              that looks the same graphlet by graphlet. <strong>Synthesis is coming soon:</strong> for now,
              this tool can run community detection on your network and gives you one zip file with your
              network and its communities.
            </p>
          </div>

          <NetworkSelection onDataChange={handleFileInputChange} />

          <div className="os-stepContainer">
            <label htmlFor="runCommunityDetection" className="os-labelWithInfo">
              <input
                type="checkbox"
                id="runCommunityDetection"
                checked={runCommunityDetection}
                onChange={(e) => setRunCommunityDetection(e.target.checked)}
                style={{ marginRight: '0.5rem' }}
              />
              Run community detection
            </label>
          </div>

          {runCommunityDetection && (
            <Options onDataChange={handleOptionChange} initialData={options} />
          )}

          <div className="os-stepContainer">
            <p>
              You will be taken to the Lookup page, where you can follow the job and download the results zip
              when it finishes.
            </p>
          </div>

          <div className="sjp-buttonContainer" role="group" aria-label="Form actions">
            <button onClick={() => navigate('/')} className="os-navButton os-navButton-previous" aria-label="Return to home page">
              &larr; Back to Home
            </button>
            <button onClick={handleSubmit} className="os-navButton" aria-label="Submit Network Synthesis job">
              Submit &rarr;
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default NetworkSynthesis;
