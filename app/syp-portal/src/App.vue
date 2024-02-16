<template>
  <!DOCTYPE html>
  <html lang="en">

  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=.6">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/css/all.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Open+Sans&display=swap" rel="stylesheet">
  </head>
    <Navbar />
  
  <body id="app body">
    <SypInfo />
  </body>
  <div id="app form">
    <Multi />
  </div>
  <div id="app footer">
    <AppFooter />
  </div>

  </html>
</template>


<script>
import Navbar from './components/Navbar.vue'
import SypInfo from './components/SypInfo.vue'
import ProductForm from './components/Multi.vue'
import AppFooter from './components/AppFooter.vue'

export default {
  components: {
    Navbar,
    SypInfo,
    Multi: ProductForm,
    AppFooter,
  }
}
</script>
