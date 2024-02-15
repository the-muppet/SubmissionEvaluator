const inputs = document.querySelectorAll('#email, #storeName, #fileInput');

function checkInputs() {
    const isFormFilled = Array.from(inputs).every(input => input.value.length > 0);
    uploadButton.disabled = !isFormFilled;
}

inputs.forEach(
    input => input.addEventListener('change', checkInputs)
);

async function uploadFile() {
    const fileInput = document.getElementById('fileInput');
    const email = document.getElementById('email').value;
    const storeName = document.getElementById('storeName').value;
    const file = fileInput.files[0];

    if (!file) {
        alert('Please select a file.');
        return;
    }
    if (!email) {
        alert('Please enter your email.');
        return;
    }
    if (!storeName) {
        alert('Please enter your store name.');
        return;
    }
    // Create FormData object
    const formData = new FormData();
    formData.append('file', file);
    formData.append('email', email);
    formData.append('storeName', storeName);
    const clientId = document.getElementById('client_id').value;
    formData.append('client_id', clientId);

    try {
        const response = await fetch('/submit/', {
            method: 'POST',
            body: formData, // Send as FormData
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const result = await response.json();
        // Display success message
        document.getElementById('messageSuccess').style.display = 'block';
        document.getElementById('messageSuccess').textContent = 'Upload successful!';
        document.getElementById('messageError').style.display = 'none'; // Hide error message

        // Update dynamic fields with results
        document.getElementById('resultStatus').textContent = `Submission Status: ${result.status || 'Success'}`;
        document.getElementById('resultValue').textContent = `Total Value: ${result.value || 'N/A'}`;
        document.getElementById('resultQuantity').textContent = `Total Quantity: ${result.quantity || 'N/A'}`;
        document.getElementById('resultAcv').textContent = `ACV: ${result.acv || 'N/A'}`;

    } catch (error) {
        console.error('Error during file upload:', error);
        // Display error message
        document.getElementById('messageError').style.display = 'block';
        document.getElementById('messageError').textContent = 'Upload failed. Please try again.';
        document.getElementById('messageSuccess').style.display = 'none'; // Hide success message

        // Reset dynamic fields
        document.getElementById('resultStatus').textContent = `Submission Status: Failed`;
        document.getElementById('resultValue').textContent = `Total Value: N/A`;
        document.getElementById('resultQuantity').textContent = `Total Quantity: N/A`;
        document.getElementById('resultAcv').textContent = `ACV: N/A`;
    }
}
