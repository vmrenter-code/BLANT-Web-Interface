// src/pages/BasicBlantRun/BasicBlantRun.tsx
// Basic BLANT Run: uploads a network and runs `./blant -k <k> network.el` on the server.
// Results appear on the Lookup page, the same way Community Detection results do.
import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import '../SubmitJob/SubmitJob.css';
import '../../components/Options/Options.css';
import NetworkSelection from '../../components/NetworkSelection';
import Processing from '../../components/Processing';
import api from '../../api/api';

// Must match BASIC_BLANT_RUN_K_MIN / _MAX in backend/types/types.ts (the server re-checks).
const K_MIN = 3;
const K_MAX = 7;
const K_DEFAULT = 4;
// When the option checkbox under the file picker is checked, k is capped at this value.
// The backend accepts 3..7, so it needs no change for this cap.
const K_MAX_WHEN_CHECKED = 5;
const kValuesUpTo = (max: number) => Array.from({ length: max - K_MIN + 1 }, (_, i) => K_MIN + i);
const MAX_FILE_BYTES = 5 * 1024 * 1024; // matches the backend upload limit

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

const BasicBlantRun: React.FC = () => {
  const navigate = useNavigate();
  const [networkFile, setNetworkFile] = useState<File | null>(null);
  const [k, setK] = useState<number>(K_DEFAULT);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  // TODO: rename once the professor says what this option is for.
  const [limitK, setLimitK] = useState<boolean>(false);
  const kMax = limitK ? K_MAX_WHEN_CHECKED : K_MAX;
  const kValues = kValuesUpTo(kMax);

  const handleLimitKChange = (checked: boolean) => {
    setLimitK(checked);
    // Checking the box sets k to the cap (5).
    if (checked) setK(K_MAX_WHEN_CHECKED);
  };

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

  const handleSubmit = async () => {
    if (!networkFile) {
      alert('Please upload a network file.');
      return;
    }
    setIsSubmitting(true);
    try {
      const formData = new FormData();
      formData.append('k', String(k));
      formData.append('file', networkFile);
      const response = await api.submitBasicBlantRun(formData);
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
      <h1 className="sjp-pageTitle">Basic BLANT Run</h1>

      {isSubmitting ? (
        <div className="sjp-accordion" role="status" aria-live="polite" aria-atomic="true">
          <Processing />
        </div>
      ) : (
        <div className="sjp-accordion" role="region" aria-label="Basic BLANT Run form">
          <NetworkSelection onDataChange={handleFileInputChange} />

          <div className="os-stepContainer">
            {/* PLACEHOLDER label/description: replace once the professor confirms the wording. */}
            <label htmlFor="blantLimitK" className="os-labelWithInfo">
              <input
                type="checkbox"
                id="blantLimitK"
                checked={limitK}
                onChange={(e) => handleLimitKChange(e.target.checked)}
                aria-describedby="blant-limit-k-description"
                style={{ marginRight: '0.5rem' }}
              />
              [Placeholder option label]
            </label>
            <p id="blant-limit-k-description">
              [Placeholder description: explain what this option does.] When checked, the largest
              graphlet size you can pick is {K_MAX_WHEN_CHECKED}.
            </p>
          </div>

          <div className="os-stepContainer">
            <div className="os-optionsGrid">
              <div className="os-inputGroup">
                <label htmlFor="blantK" className="os-labelWithInfo">
                  Graphlet Size (k)
                </label>
                <select
                  id="blantK"
                  className="os-selectInput"
                  value={k}
                  onChange={(e) => setK(Number(e.target.value))}
                  aria-describedby="blant-k-description"
                >
                  {kValues.map((value) => (
                    <option key={value} value={value}>{value}</option>
                  ))}
                </select>
                <span id="blant-k-description" className="sr-only">
                  Number of nodes in the graphlets BLANT samples, between {K_MIN} and {kMax}
                </span>
              </div>
            </div>
            <p>
              Runs <code>blant -k {k} {networkFile ? networkFile.name : 'your_network.el'}</code> on our server.
              You will be taken to the Lookup page, where the output appears.
            </p>
          </div>

          <div className="sjp-buttonContainer" role="group" aria-label="Form actions">
            <button onClick={() => navigate('/')} className="os-navButton os-navButton-previous" aria-label="Return to home page">
              &larr; Back to Home
            </button>
            <button onClick={handleSubmit} className="os-navButton" aria-label="Submit Basic BLANT Run job">
              Submit &rarr;
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default BasicBlantRun;
