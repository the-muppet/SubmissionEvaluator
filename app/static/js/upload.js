let convertedCsvData = null;

document.getElementById('fileUpload').addEventListener('change', handleFile, false);

function handleFile(e) {
    const file = e.target.files[0];
    if (file) {
        const reader = new FileReader();
        reader.onload = function (e) {
            const data = new Uint8Array(e.target.result);
            const workbook = XLSX.read(data, { type: 'array' });
            const firstSheetName = workbook.SheetNames[0];
            const worksheet = workbook.Sheets[firstSheetName];
            convertedCsvData = XLSX.utils.sheet_to_csv(worksheet);
            document.getElementById('submitBtn').disabled = false;
        };
        reader.readAsArrayBuffer(file);
    }
}

document.getElementById('uploadForm').addEventListener('submit', async function (event) {
    event.preventDefault();
    const email = document.getElementById('email').value;
    const storeName = document.getElementById('storeName').value;
    const clientId = document.getElementById('client_id').value;

    if (!convertedCsvData) {
        alert('Please select a file and wait for it to be processed.');
        return;
    }

    const formData = new FormData();
    const csvBlob = new Blob([convertedCsvData], { type: 'text/csv' });
    formData.append('file', csvBlob, `${storeName}_${Date.now()}.csv`);
    formData.append('email', email);
    formData.append('storeName', storeName);
    formData.append('client_id', clientId);

    try {
        const response = await fetch('/submit/', {
            method: 'POST',
            body: formData,
        });
        const result = await response.json();

        document.getElementById('message').textContent = response.ok ? 'Upload successful!' : `Upload failed: ${result.message}`;
        document.getElementById('message').style.color = response.ok ? 'green' : 'red';
    } catch (error) {
        console.error('Upload failed:', error);
        document.getElementById('message').textContent = 'Upload failed. Please try again.';
        document.getElementById('message').style.color = 'red';
    }
});