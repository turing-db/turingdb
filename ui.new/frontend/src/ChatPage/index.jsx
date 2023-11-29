// Core
import * as React from "react";

import { useChatTheme } from "src/ChatPage/theme";

// @blueprintjs
import { Button, Icon, TextArea, Card, Spinner } from "@blueprintjs/core";

const Message = (props) => {
  const {
    name,
    userMessageBg,
    aiMessageBg,
    iconBackgroundColor,
    text,
    iconSize,
    iconPadding,
    userNameSize,
  } = useChatTheme();

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: props.ai ? "start" : "end",
        padding: 5,
      }}>
      <div
        style={{
          display: "flex",
          flexDirection: "row",
          width: "maxContent",
          alignItems: "center",
        }}>
        {props.ai || (
          <div
            style={{
              fontSize: userNameSize,
              padding: 5,
              color: name,
            }}>
            You
          </div>
        )}
        <Icon
          icon={props.ai ? "desktop" : "user"}
          size={iconSize}
          style={{
            marginBottom: 5,
            padding: iconPadding,
            backgroundColor: iconBackgroundColor,
            borderRadius: "50%",
            color: props.ai ? aiMessageBg : userMessageBg,
            display: "block",
          }}
        />
        {props.ai && (
          <div
            style={{
              fontSize: userNameSize,
              padding: 5,
              color: name,
            }}>
            Assistant
          </div>
        )}
      </div>
      <Card
        style={{
          width: "max-content",
          padding: "12px 20px 12px 20px",
          color: text,
          backgroundColor: props.ai ? aiMessageBg : userMessageBg,
          maxWidth: "100%",
        }}>
        <code
          style={{ fontSize: 13 }}
          dangerouslySetInnerHTML={{ __html: props.content }}></code>
      </Card>
    </div>
  );
};

const MessageList = ({ messages, loading }) => {
  const { brainIconOpacity, brainIconColor } = useChatTheme();

  const defaultStyle = {
    display: "flex",
    minHeight: "60vh",
    marginBottom: 20,
  };

  return messages.length === 0 ? (
    <Card
      elevation={4}
      style={{
        ...defaultStyle,
        alignItems: "center",
        justifyContent: "center",
      }}>
      {loading ? (
        <Spinner
          size={100}
          color={brainIconColor}
          style={{
            padding: 10,
          }}
        />
      ) : (
        <Icon
          icon="desktop"
          size={100}
          color={brainIconColor}
          style={{
            padding: 10,
            opacity: brainIconOpacity,
          }}
        />
      )}
    </Card>
  ) : (
    <Card
      elevation={4}
      style={{
        ...defaultStyle,
        flexDirection: "column",
        justifyContent: "end",
      }}>
      {messages.map((msg, i) => (
        <Message key={"msg-" + i} content={msg.content} ai={msg.ai} />
      ))}
    </Card>
  );
};

const Prompt = ({ loading, messages, setMessages }) => {
  const [currentMsg, setCurrentMsg] = React.useState("");

  React.useEffect(() => {
    window.scrollTo(0, document.body.scrollHeight);
  }, [messages]);

  const submit = React.useCallback(() => {
    setMessages([
      ...messages,
      {
        content: currentMsg,
        ai: false,
      },
    ]);
    setCurrentMsg("");
  }, [currentMsg, messages, setMessages]);

  return (
    <Card
      elevation={4}
      style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        padding: 10,
      }}>
      <TextArea
        id="current-msg-prompt"
        asyncControl={true}
        disabled={loading}
        fill
        style={{ padingLeft: 10, height: "8vh", resize: "none" }}
        value={currentMsg}
        placeholder="Ask me anything..."
        onChange={(e) => setCurrentMsg(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            submit();
          }
        }}
      />
      <div style={{ paddingLeft: 10 }}>
        <Button
          icon="send-message"
          disabled={currentMsg.length === 0}
          onClick={submit}
        />
      </div>
    </Card>
  );
};

const useChatMessages = () => {
  const [messages, setMessages] = React.useState([]);
  const [preset, setPreset] = React.useState({});
  const [loading, setLoading] = React.useState(true);

  // Initialize preset answers
  React.useEffect(() => {
    fetch("chat-answers.json").then((res) =>
      res.json().then((json) => {
        setPreset(json);
        setLoading(false);
      })
    );
  }, []);

  // React to message received
  React.useEffect(() => {
    const lastMessage = messages.slice(-1)[0];
    if (!lastMessage || lastMessage.ai) return; // If it is an AI answer, return

    const answer =
      preset[lastMessage.content] ||
      "Sorry but I did not understand your question";

    setMessages([...messages, { content: answer, ai: true }]);
  }, [preset, messages]);

  return {
    loading,
    messages,
    setMessages,
  };
};
const ChatPage = () => {
  const { loading, messages, setMessages } = useChatMessages();

  return (
    <div style={{ padding: 30, maxWidth: "100vh", overflow: "hidden" }}>
      <MessageList messages={messages} loading={loading} />
      <Prompt loading={loading} messages={messages} setMessages={setMessages} />
    </div>
  );
};
export default ChatPage;
