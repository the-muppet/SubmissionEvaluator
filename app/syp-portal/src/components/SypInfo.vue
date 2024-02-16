<template>
  <div class="container mx-auto p-6">
    <div class="text-center mb-4">
      <h1 class="font-bold text-2xl mb-2">Store Your Products Approval Form</h1>
    </div>

    <div class="mb-6">
      <p class="text-sm">
        <strong>PLEASE NOTE:</strong> SYP approval criteria HAS BEEN UPDATED*<br />
        If you have additional questions please reach out to
        <a href="mailto:syp@tcgplayer.com" class="text-blue-600">syp@tcgplayer.com</a>.
      </p>
      <p class="text-xs mt-2">
        Rejected or unscheduled shipments are subject to refusal by our Authentication Center.
      </p>
    </div>

    <div class="flex justify-center mb-6">
      <div class="mb-4">
        <h4>SUBMIT WITH CONFIDENCE!!</h4>
        <p>
          Our enhanced Submission Evaluator efficiently and seamlessly corrects typical data entry
          errors like mismatched IDs and typos. This improvement ensures a more user-friendly
          experience and peace of mind, delivering swift results. Benefit from quicker approval
          processes, reduced turnaround times, and the potential for increased earnings.
        </p>
      </div>
      <div class="mb-4">
        <h2 class="font-bold text-lg mb-2">Shipment Requirements</h2>
        <table class="text-lg">
          <thead>
            <tr>
              <th class="text-center font-bold px-2 py-1 border-b-2">Match %</th>
              <th class="text-center font-bold px-2 py-1 border-b-2">Under $2.00</th>
              <th class="text-center font-bold px-2 py-1 border-b-2">$2.00 - $3.00</th>
              <th class="text-center font-bold px-2 py-1 border-b-2">Over $3.00</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td class="text-center px-2 py-1 border-b">0% - 50%</td>
              <td class="text-center px-2 py-1 border-b">Refused</td>
              <td class="text-center px-2 py-1 border-b">Refused</td>
              <td class="text-center px-2 py-1 border-b">Accepted</td>
            </tr>
            <tr>
              <td class="text-center px-2 py-1 border-b">51% - 75%</td>
              <td class="text-center px-2 py-1 border-b">Refused</td>
              <td class="text-center px-2 py-1 border-b">Accepted</td>
              <td class="text-center px-2 py-1 border-b">Accepted</td>
            </tr>
            <tr>
              <td class="text-center px-2 py-1 border-b">76% - 100%</td>
              <td class="text-center px-2 py-1 border-b">Refused</td>
              <td class="text-center px-2 py-1 border-b">Accepted</td>
              <td class="text-center px-2 py-1 border-b">Accepted</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'SypInfo'
}
</script>

<style scoped>


</style>
