// Core
import { useTheme } from "@emotion/react";
import * as React from "react";

// @blueprintjs
import { Text, Button, Icon, TextArea, Card } from "@blueprintjs/core";

const useChatTheme = () => {
  const palette = useTheme().palette;

  const generalTheme = {
    aiMessageBg: "#0F6894",
    userMessageBg: "#007067",
  };

  return palette.mode === "dark"
    ? {
        ...generalTheme,
        bg: "#050505",
        promptBg: "#151522",
        brainIconColor: "#555",
        brainIconOpacity: 0.1,
      }
    : {
        ...generalTheme,
        bg: "#fff",
        prompBg: "f8f8f8",
        brainIconColor: "#555",
        brainIconOpacity: 0.1,
      };
};

const Message = (props) => {
  const { userMessageBg, aiMessageBg } = useChatTheme();

  return (
    <div
      style={{
        display: "flex",
        flexDirection: props.ai ? "row" : "row-reverse",
        alignItems: "center",
        justifyContent: props.ai ? "left" : "right",
      }}>
      <Icon
        icon={props.ai ? "desktop" : "user"}
        size={30}
        style={{
          padding: "0px 9px 0px 9px",
          color: props.ai ? aiMessageBg : userMessageBg,
        }}></Icon>
      <Card
        style={{
          width: "max-content",
          padding: 5,
          borderRadius: 5,
          backgroundColor: props.ai ? aiMessageBg : userMessageBg,
        }}>
        <code style={{ fontSize: 13 }}>{props.content}</code>
      </Card>
    </div>
  );
};

const MessageList = ({ messages }) => {
  const { bg, brainIconOpacity, brainIconColor } = useChatTheme();
  console.log({messages})

  const defaultStyle = {
    display: "flex",
    minHeight: "60vh",
    marginBottom: 20,
  };

  return messages.length === 0 ? (
    <Card
      style={{
        ...defaultStyle,
        alignItems: "center",
        justifyContent: "center",
      }}>
      <Icon
        icon="desktop"
        size={100}
        color={brainIconColor}
        style={{
          padding: 10,
          opacity: brainIconOpacity,
        }}
      />
    </Card>
  ) : (
    <Card
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

const Prompt = ({ messages, setMessages }) => {
  const [currentMsg, setCurrentMsg] = React.useState("");

  return (
    <Card
      style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        padding: 10,
      }}>
      <TextArea
        asyncControl={true}
        rightIcon="send-message"
        fill
        style={{ padingLeft: 10, height: "15vh", resize: "none" }}
        content={currentMsg}
        ellipsize
        placeholder="Ask me anything..."
        onChange={(e) => setCurrentMsg(e.target.value)}
      />
      <div style={{ paddingLeft: 10 }}>
        <Button
          icon="send-message"
          onClick={() => {
            setMessages([
              ...messages,
              {
                content: currentMsg,
                ai: false,
              },
            ]);
            setCurrentMsg("");
          }}
        />
      </div>
    </Card>
  );
};

const ChatPage = () => {
  const [messages, setMessages] = React.useState([
    { content: "test user question", ai: false },
    { content: "response", ai: true },
  ]);

  return (
    <div style={{ padding: 30 }}>
      <MessageList messages={messages} />
      <Prompt messages={messages} setMessages={setMessages} />
    </div>
  );
};
export default ChatPage;
