import React, { useState } from "react";
import "./App.css";
import executeQuery from './firebase_query'; // Import executeQuery

function App() {
  const [messages, setMessages] = useState([]);
  const [inputText, setInputText] = useState("");
  const [selectedFile, setSelectedFile] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);

  // Predefined responses
  const responses = {
    "hi": "Hi! How can I help you today?",
    "hello": "Hello! How can I assist you today?",
    Hey: "Hey! How can I help?",
    "Whats up": "Nothing much, how can I help you today?",
    "Good morning": "Good morning! What can I do for you today?",
    "Good afternoon": "Good afternoon! How can I assist?",
    "Good evening": "Good evening! How may I help?",
    "whats your name": "My name is ChatDB.",
    "what are you":
      "I’m ChatDB, your assistant for querying databases and analyzing data. I can help you with SQL queries, Firebase data, and much more!",
    "who are you":
      "I’m ChatDB, your assistant for querying databases and analyzing data. I can help you with SQL queries, Firebase data, and much more!",
    "what do you do":
      "I help with querying databases, analyzing data, and explaining concepts in natural language.",
    "why are you here":
      "I’m here to assist you with database tasks, answer questions, and make your data analysis simpler!",
    "bye": "Goodbye! Have a great day!",
    "See you": "See you later! Let me know if you need help again.",
    Goodbye: "Goodbye! Feel free to come back if you have more questions.",
    "Thanks, bye": "You’re welcome! Take care!",
    "Catch you later": "Catch you later! Have a good one!",
  };


  const exampleQueries = {
    "select": {
        "example_1": {
            "query": "SELECT * FROM {table_name};",
            "explanation": "Retrieve all columns and rows from the table named '{table_name}'."
        },
        "example_2": {
            "query": "SELECT {column1}, {column2} FROM {table_name};",
            "explanation": "Retrieve the '{column1}' and '{column2}' columns from the table named '{table_name}'."
        },
        "example_3": {
            "query": "SELECT {column1} AS {alias1}, {column2} AS {alias2} FROM {table_name};",
            "explanation": "Retrieve '{column1}' and '{column2}' from the table '{table_name}', displaying them as '{alias1}' and '{alias2}' respectively."
        },
        "example_4": {
            "query": "SELECT DISTINCT {column1} FROM {table_name};",
            "explanation": "Retrieve unique values from '{column1}' in '{table_name}'."
        },
        "example_5": {
            "query": "SELECT {column1}, COUNT({column2}) AS count_value FROM {table_name} GROUP BY {column1};",
            "explanation": "Retrieve '{column1}' and the count of '{column2}' grouped by '{column1}' from '{table_name}'."
        }
    },
    "where": {
        "example_1": {
            "query": "SELECT * FROM {table_name} WHERE {column1} = 'value_1';",
            "explanation": "Retrieve all rows from the table '{table_name}' where '{column1}' has the value 'value_1'."
        },
        "example_2": {
            "query": "SELECT * FROM {table_name} WHERE {column1} LIKE 'prefix%';",
            "explanation": "Retrieve rows where '{column1}' starts with 'prefix'."
        },
        "example_3": {
            "query": "SELECT * FROM {table_name} WHERE {column1} BETWEEN 10 AND 20;",
            "explanation": "Retrieve rows where '{column1}' is between 10 and 20."
        },
        "example_4": {
            "query": "SELECT * FROM {table_name} WHERE {column1} IS NULL;",
            "explanation": "Retrieve rows where '{column1}' has no value (NULL)."
        },
        "example_5": {
            "query": "SELECT * FROM {table_name} WHERE {column1} IN ('value_1', 'value_2');",
            "explanation": "Retrieve rows where '{column1}' matches any value in the specified list."
        }
    },
    "group_by": {
        "example_1": {
            "query": "SELECT {column1}, COUNT(*) FROM {table_name} GROUP BY {column1};",
            "explanation": "Group rows in the table '{table_name}' by the '{column1}' value and return each unique value along with the count of rows for that value."
        },
        "example_2": {
            "query": "SELECT {column1}, AVG({column2}) FROM {table_name} GROUP BY {column1};",
            "explanation": "Group rows in '{table_name}' by '{column1}' and calculate the average of '{column2}' for each group."
        },
        "example_3": {
            "query": "SELECT {column1}, MAX({column2}) FROM {table_name} GROUP BY {column1};",
            "explanation": "Retrieve '{column1}' and the maximum value of '{column2}' grouped by '{column1}' in '{table_name}'."
        },
        "example_4": {
            "query": "SELECT {column1}, MIN({column2}) FROM {table_name} GROUP BY {column1};",
            "explanation": "Retrieve '{column1}' and the minimum value of '{column2}' grouped by '{column1}' in '{table_name}'."
        },
        "example_5": {
            "query": "SELECT {column1}, SUM({column2}) FROM {table_name} GROUP BY {column1} HAVING SUM({column2}) > 100;",
            "explanation": "Group rows in '{table_name}' by '{column1}', calculate the sum of '{column2}' for each group, and retrieve only groups where the sum is greater than 100."
        }
    },
    "having": {
        "example_1": {
            "query": "SELECT {column1}, COUNT(*) FROM {table_name} GROUP BY {column1} HAVING COUNT(*) > 10;",
            "explanation": "Retrieve groups from '{table_name}' where the count of rows in each group exceeds 10."
        },
        "example_2": {
            "query": "SELECT {column1}, SUM({column2}) FROM {table_name} GROUP BY {column1} HAVING SUM({column2}) < 100;",
            "explanation": "Retrieve groups where the sum of '{column2}' is less than 100."
        },
        "example_3": {
            "query": "SELECT {column1}, AVG({column2}) FROM {table_name} GROUP BY {column1} HAVING AVG({column2}) > 50;",
            "explanation": "Retrieve groups where the average of '{column2}' is greater than 50."
        },
        "example_4": {
            "query": "SELECT {column1}, MAX({column2}) FROM {table_name} GROUP BY {column1} HAVING MAX({column2}) = 100;",
            "explanation": "Retrieve groups where the maximum value of '{column2}' is 100."
        },
        "example_5": {
            "query": "SELECT {column1}, MIN({column2}) FROM {table_name} GROUP BY {column1} HAVING MIN({column2}) < 20;",
            "explanation": "Retrieve groups where the minimum value of '{column2}' is less than 20."
        }
    },
    "order_by": {
        "example_1": {
            "query": "SELECT * FROM {table_name} ORDER BY {column1};",
            "explanation": "Retrieve all rows from '{table_name}' and sort them by '{column1}' in ascending order."
        },
        "example_2": {
            "query": "SELECT * FROM {table_name} ORDER BY {column1} DESC;",
            "explanation": "Retrieve all rows from '{table_name}' and sort them by '{column1}' in descending order."
        },
        "example_3": {
            "query": "SELECT * FROM {table_name} ORDER BY {column1}, {column2};",
            "explanation": "Sort rows in '{table_name}' by '{column1}' and then by '{column2}' in ascending order."
        },
        "example_4": {
            "query": "SELECT * FROM {table_name} ORDER BY {column1} ASC, {column2} DESC;",
            "explanation": "Sort rows by '{column1}' in ascending order and '{column2}' in descending order."
        },
        "example_5": {
            "query": "SELECT * FROM {table_name} ORDER BY LENGTH({column1});",
            "explanation": "Sort rows by the length of the values in '{column1}'."
        }
    },
    "join": {
        "example_1": {
            "query": "SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} INNER JOIN {table2} ON {table1}.{column3} = {table2}.{column3};",
            "explanation": "Combine rows from '{table1}' and '{table2}' where '{column3}' in both tables match."
        },
        "example_2": {
            "query": "SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} LEFT JOIN {table2} ON {table1}.{column3} = {table2}.{column3};",
            "explanation": "Retrieve all rows from '{table1}' and matching rows from '{table2}', leaving unmatched rows from '{table1}'."
        },
        "example_3": {
            "query": "SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} RIGHT JOIN {table2} ON {table1}.{column3} = {table2}.{column3};",
            "explanation": "Retrieve all rows from '{table2}' and matching rows from '{table1}', leaving unmatched rows from '{table2}'."
        },
        "example_4": {
            "query": "SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} FULL OUTER JOIN {table2} ON {table1}.{column3} = {table2}.{column3};",
            "explanation": "Combine rows from both tables, retrieving all rows where matches occur or not."
        },
        "example_5": {
            "query": "SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} CROSS JOIN {table2};",
            "explanation": "Combine all rows from '{table1}' with all rows from '{table2}'."
        }
    }
}

const formatWithLineBreaks = (text) => {
  return text.split("\n").map((line, idx) => (
    <React.Fragment key={idx}>
      {line}
      <br />
    </React.Fragment>
  ));
};

  const handleTextChange = (event) => {
    setInputText(event.target.value);
  };

  const handleSend = async () => {
    if (!inputText) {
      alert("Please enter a message.");
      return;
    }
  
    // Add user's text message to the chat
    setMessages((prevMessages) => [
      ...prevMessages,
      { sender: "user", text: inputText },
    ]);
  
    // Predefined responses
    const userMessage = inputText.trim().toLowerCase();
    if (responses[userMessage]) {
      setMessages((prevMessages) => [
        ...prevMessages,
        { sender: "bot", text: responses[userMessage] },
      ]);
      setInputText("");
      return;
    }
  
    // Check for "example"
    if (userMessage === "example") {
      const allExamples = Object.values(exampleQueries)
        .flatMap((examples) => Object.values(examples));
      const randomExamples = allExamples
        .sort(() => 0.5 - Math.random())
        .slice(0, 5);
  
      const exampleTexts = randomExamples
        .map((ex, idx) => `${idx + 1}. ${ex.query}\nExplanation:\n${ex.explanation}`)
        .join("\n\n");
  
      setMessages((prevMessages) => [
        ...prevMessages,
        { sender: "bot", text: `Here are 5 random examples:\n\n${exampleTexts}` },
      ]);
      setInputText("");
      return;
    }
  
    // Check for "<key> example"
    const [key, ...rest] = userMessage.split(" ");
    if (rest.join(" ") === "example" && exampleQueries[key]) {
      const examples = exampleQueries[key];
      const exampleTexts = Object.values(examples)
        .map((ex, idx) => `${idx + 1}. ${ex.query}\nExplanation:\n${ex.explanation}`)
        .join("\n\n");
  
      setMessages((prevMessages) => [
        ...prevMessages,
        { sender: "bot", text: `Examples for '${key}':\n\n${exampleTexts}` },
      ]);
      setInputText("");
      return;
    }







    // Fallback to backend API call
    try {
      const res = await fetch("http://127.0.0.1:5000/analyze", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ text: inputText }),
      });

      const data = await res.json();

      // Add backend response to the chat
      if (data.query) {
        setMessages((prevMessages) => [
          ...prevMessages,
          { sender: "bot", text: data.query },
        ]);
      } else {
        setMessages((prevMessages) => [
          ...prevMessages,
          { sender: "bot", text: "I didn't understand that. Please try again!" },
        ]);
      }
    } catch (error) {
      console.error("Error:", error);
      setMessages((prevMessages) => [
        ...prevMessages,
        { sender: "bot", text: "Error processing your request." },
      ]);
    }

    setInputText("");
  };

  const handleFileChange = (event) => {
    setSelectedFile(event.target.files[0]);
  };

  const handleUpload = async (target) => {
    if (!selectedFile) {
      alert("Please select a file to upload.");
      return;
    }

    const formData = new FormData();
    formData.append("file", selectedFile);

    try {
      const endpoint =
        target === "mysql"
          ? "http://127.0.0.1:5000/sql/create"
          : "http://127.0.0.1:5000/firebase/create";

      const res = await fetch(endpoint, {
        method: "POST",
        body: formData,
      });

      const data = await res.json();

      // Add backend response to the chat
      setMessages((prevMessages) => [
        ...prevMessages,
        {
          sender: "bot",
          text: data.message || "Unexpected response from server.",
        },
      ]);
    } catch (error) {
      console.error("Upload error:", error);
      setMessages((prevMessages) => [
        ...prevMessages,
        { sender: "bot", text: "Error uploading file." },
      ]);
    }

    setSelectedFile(null);
    setIsModalOpen(false); // Close modal
  };

  return (
    <div className="App">
      <div className="chat-wrapper">
        <header className="chat-header">
          <h1>ChatDB</h1>
          <p>Your AI-powered assistant for databases</p>
        </header>
        <div className="chat-content">
          {messages.map((msg, index) => (
            <div
              key={index}
              className={`message ${
                msg.sender === "user" ? "user-message" : "bot-message"
              }`}
            >
              {msg.sender === "bot" ? formatWithLineBreaks(msg.text) : msg.text}
            </div>
          ))}
        </div>
        <div className="chat-input-area">
          <textarea
            className="chat-input"
            placeholder="Type your message..."
            value={inputText}
            onChange={handleTextChange}
          />
          <button className="send-button" onClick={handleSend}>
            Send
          </button>
          <button
            className="file-upload-button"
            onClick={() => setIsModalOpen(true)}
          >
            Upload
          </button>
        </div>
      </div>

      {/* Modal for upload options */}
      {isModalOpen && (
        <div className="upload-modal active">
          <div className="modal-content">
            <button
              className="modal-close"
              onClick={() => setIsModalOpen(false)}
            >
              &times;
            </button>
            <h2>Upload File</h2>
            <input type="file" onChange={handleFileChange} />
            <div className="modal-buttons">
              <button
                className="modal-button mysql"
                onClick={() => handleUpload("mysql")}
              >
                Upload to MySQL
              </button>
              <button
                className="modal-button firebase"
                onClick={() => handleUpload("firebase")}
              >
                Upload to Firebase
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
