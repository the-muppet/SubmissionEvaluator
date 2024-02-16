async function fetchClientId(email) {
    try {
        const response = await fetch('/get-client-id/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ email }),
        });
        if (response.ok) {
            const data = await response.json();
            if (data.client_id) {
                console.log(`Recieved client id: ${data.client_id}`)
                return data.client_id;
            } else {
                console.log(`Error: ${response.status}`)
                throw new Error(`Error: ${response.status}`);
            }
        }
    } catch (error) {
        console.error('Error:', error);
    }
}


document.getElementById('email').addEventListener('submit', async function (event) {
    event.preventDefault();
    const email = document.getElementById('email').value;
    const clientId = await fetchClientId(email);
    if (clientId) {
        console.log('Client ID:', clientId);
        const ws = new WebSocket(`ws://localhost:8000/ws/${clientId}`);
        ws.onmessage = function (event) {
            const message = event.data;
            console.log("Message from server ", message);
            document.getElementById("message").innerHTML = message;
            document.getElementById("message").style.display = "block";
        };
        ws.onopen = function (event) {
            ws.send("Hello Server!");
        };
    }
});
