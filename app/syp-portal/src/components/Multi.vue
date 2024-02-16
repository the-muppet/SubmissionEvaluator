<template>
  <div class="container">
    <form @submit.prevent="handleSubmit">
      <div class="mb-3">
        <label for="storeName" class="block text-sm font-bold mb-2">
          Store Your Products (SYP) store name as it appears on TCGplayer*
        </label>
        <input type="text" id="storeName" v-model="form.storeName" class="border p-2 w-full" />
      </div>

      <div class="mb-3">
        <label for="email" class="block text-sm font-bold mb-2">
          Email associated with your SYP account*
        </label>
        <input type="email" id="email" v-model="form.email" class="border p-2 w-full" />
      </div>

      <div class="grid grid-cols-2 gap-4 mb-3">
        <div>
          <label for="firstName" class="block text-sm font-bold mb-2">First name *</label>
          <input type="text" id="firstName" v-model="form.firstName" class="border p-2 w-full" />
        </div>
        <div>
          <label for="lastName" class="block text-sm font-bold mb-2">Last name *</label>
          <input type="text" id="lastName" v-model="form.lastName" class="border p-2 w-full" />
        </div>
      </div>

      <div class="mb-6">
        <label for="fileUpload" class="block text-sm font-bold mb-2">
          Magic: the Gathering Cards - upload your sorted and ready to ship MTG products *
        </label>
        <input
          type="file"
          id="fileUpload"
          accept=".xlsx, .xls, .csv, .ods"
          @change="handleFile($event, 'mtg')"
          class="border p-2 w-full"
        />
      </div>

      <div class="mb-6">
        <label for="fileUploadPk" class="block text-sm font-bold mb-2">
          Pokémon Cards - upload your sorted and ready to ship Pokémon products *
        </label>
        <input
          type="file"
          id="fileUploadPk"
          accept=".xlsx, .xls, .csv, .ods"
          @change="handleFile($event, 'pokemon')"
          class="border p-2 w-full"
        />
      </div>

      <div class="text-center">
        <button
          type="submit"
          id="submitBtn"
          :disabled="!convertedCsvData"
          class="bg-blue-600 text-white px-4 py-2 rounded w-full"
        >
          Submit
        </button>
      </div>
    </form>
    <div id="message" :style="{ color: messageColor }">{{ submissionMessage }}</div>
  </div>
</template>

<script>
import * as XLSX from 'xlsx'

export default {
  data() {
    return {
      form: {
        storeName: '',
        email: '',
        firstName: '',
        lastName: ''
      },
      convertedCsvData: null,
      submissionMessage: '',
      messageColor: 'black', // Default message color
      ws: null,
      wsMessage: '' // WebSocket message
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
          return data.client_id ? data.client_id : null
        } else {
          throw new Error(`Error fetching client ID: ${response.statusText}`)
        }
      } catch (error) {
        console.error('Error:', error)
        return null
      }
    },
    handleFile(event) {
      const file = event.target.files[0]
      if (!file) return

      const reader = new FileReader()
      reader.onload = (e) => {
        const data = new Uint8Array(e.target.result)
        const workbook = XLSX.read(data, { type: 'array' })
        const firstSheetName = workbook.SheetNames[0]
        const worksheet = workbook.Sheets[firstSheetName]
        this.convertedCsvData = XLSX.utils.sheet_to_csv(worksheet)
      }
      reader.readAsArrayBuffer(file)
    },
    async handleSubmit() {
      if (!this.convertedCsvData) {
        this.submissionMessage = 'Please select a file and wait for it to be processed.'
        this.messageColor = 'red'
        return
      }

      const clientId = await this.fetchClientId(this.form.email)
      if (clientId) {
        this.initializeWebSocket(clientId)
      } else {
        this.submissionMessage = 'Failed to obtain client ID.'
        this.messageColor = 'red'
        return
      }

      const formData = new FormData()
      const csvBlob = new Blob([this.convertedCsvData], { type: 'text/csv' })
      formData.append('file', csvBlob, `${this.form.storeName}_${Date.now()}.csv`)
      formData.append('email', this.form.email)
      formData.append('storeName', this.form.storeName)

      try {
        const response = await fetch('/submit/', {
          method: 'POST',
          body: formData
        })
        const result = await response.json()

        this.submissionMessage = response.ok
          ? 'Upload successful!'
          : `Upload failed: ${result.message}`
        this.messageColor = response.ok ? 'green' : 'red'
      } catch (error) {
        console.error('Upload failed:', error)
        this.submissionMessage = 'Upload failed. Please try again.'
        this.messageColor = 'red'
      }
    },
    initializeWebSocket(clientId) {
      this.ws = new WebSocket(`ws://localhost:8000/ws/${clientId}`)
      this.ws.onmessage = (event) => {
        this.wsMessage = event.data
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
