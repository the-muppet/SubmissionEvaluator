<template>
  <nav class="container-fluid">
    <ul>
      <li><strong>File Submission Portal</strong></li>
    </ul>
    <ul>
      <li><a href="#" @click.prevent="navigate('home')">Home</a></li>
      <li><a href="#" @click.prevent="navigate('about')">About</a></li>
      <li><a href="#" @click.prevent="navigate('sypView')">SYP View</a></li>
      <li><a href="#" @click.prevent="navigate('contactUs')">Contact Us</a></li>
    </ul>
  </nav>
</template>

<script>
export default {
  methods: {
    navigate(view) {
      console.log(`Navigate to ${view}`)
    }
  }
}
</script>
<style scoped>
nav ul {
  list-style-type: none;
  padding: 0;
}

nav ul li {
  position: sticky;
  bottom: 100%;
  margin-left: 80%
}

nav ul li a {
  text-decoration: none;
  color: rgb(12, 177, 34);
  /* Adjust color as needed */
}

nav ul li a:hover {
  text-decoration: underline;
}
</style>
