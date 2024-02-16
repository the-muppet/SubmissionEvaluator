<template>
  <div>
    <form @submit.prevent="handleSubmit">
      <input type="email" id="email" v-model="email" placeholder="Enter your email" />
      <button type="submit">Submit</button>
    </form>
    <div id="message" v-if="message">{{ message }}</div>
  </div>
</template>

<script>
export default {
  data() {
    return {
      email: '',
      message: '',
      ws: null
    }
  },
  methods: {
    async fetchClientId(email) {
      try {
        const response = await fetch('/get-client-id/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email })
        })
        if (response.ok) {
          const data = await response.json()
          if (data.client_id) {
            console.log(`Received client id: ${data.client_id}`)
            return data.client_id
          } else {
            console.log(`Error: ${response.status}`)
            throw new Error(`Error: ${response.status}`)
          }
        }
      } catch (error) {
        console.error('Error:', error)
      }
    },
    async handleSubmit() {
      const clientId = await this.fetchClientId(this.email)
      if (clientId) {
        console.log('Client ID:', clientId)
        this.initializeWebSocket(clientId)
      }
    },
    initializeWebSocket(clientId) {
      this.ws = new WebSocket(`ws://localhost:8000/ws/${clientId}`)
      this.ws.onmessage = (event) => {
        this.message = event.data
        console.log('Message from server:', this.message)
      }
      this.ws.onopen = () => {
        this.ws.send('Hello Server!')
      }
    }
  },
  beforeUnmount() {
    if (this.ws) {
      this.ws.close()
    }
  }
}
</script>
