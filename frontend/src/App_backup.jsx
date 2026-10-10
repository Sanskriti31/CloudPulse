
import { useEffect, useState } from "react";
import "./App.css";

function App() {
  const [users, setUsers] = useState([]);
  const [status, setStatus] = useState("Loading...");
  const [error, setError] = useState("");

  useEffect(() => {
    fetch("http://localhost:8000/api/test-cassandra/")
      .then((res) => {
        if (!res.ok) throw new Error("Backend request failed");
        return res.json();
      })
      .then((data) => {
        setUsers(data.users);
        setStatus("Connected to Cassandra");
      })
      .catch((err) => {
        setError(err.message);
        setStatus("Connection failed");
      });
  }, []);

  return (
    <main>
      <h1>DBMS Project</h1>
      <p>React + Django + Apache Cassandra</p>
      <h2>{status}</h2>

      {error && <p>{error}. Check Django and CORS settings.</p>}

      {users.map((user) => (
        <section key={user.user_id}>
          <h3>{user.name}</h3>
          <p>{user.email}</p>
        </section>
      ))}
    </main>
  );
}

export default App;
