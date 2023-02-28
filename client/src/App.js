import React, { useState, useEffect } from 'react';
// import axios from 'axios';
import Dashboard from './components/Dashboard';

// function App() {
//   const [message, setMessage] = useState('');

//   useEffect(() => {
//     axios.get('http://localhost:5000/api')
//       .then(response => setMessage(response.data.message))
//       .catch(error => console.log(error));
//   }, []);

//   return (
//     <div>
//       <h1>{message}</h1>
//     </div>
//   );
// }

function App() {
  return (
    <div className="App">
      <Dashboard />
    </div>
  );
}

export default App;