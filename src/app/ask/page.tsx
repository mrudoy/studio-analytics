"use client";

import { useChat } from "@ai-sdk/react";
import { useRef } from "react";
import Link from "next/link";
import { ArrowLeft, Send, Square, Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  PromptInput,
  PromptInputTextarea,
  PromptInputActions,
  PromptInputAction,
} from "@/components/prompt-kit/prompt-input";
import { Message, MessageContent } from "@/components/prompt-kit/message";
import { PromptSuggestion } from "@/components/prompt-kit/prompt-suggestion";
import {
  ChatContainerRoot,
  ChatContainerContent,
  ChatContainerScrollAnchor,
} from "@/components/prompt-kit/chat-container";
import { ScrollButton } from "@/components/prompt-kit/scroll-button";
import { TextShimmerLoader } from "@/components/prompt-kit/loader";

const SUGGESTIONS = [
  "How is revenue trending this year vs last year?",
  "What's our current MRR and subscriber count?",
  "Break down churn rates by membership tier",
  "How is the merch store performing?",
  "Who uses the spa — are they members?",
  "What does our conversion funnel look like?",
];

export default function AskPage() {
  const { messages, input, setInput, handleSubmit, isLoading, stop } = useChat({
    api: "/api/chat",
  });
  const formRef = useRef<HTMLFormElement>(null);

  const handleSuggestionClick = (suggestion: string) => {
    setInput(suggestion);
    // Submit after a tick so the input value is set
    setTimeout(() => {
      formRef.current?.requestSubmit();
    }, 0);
  };

  const handleKeySubmit = () => {
    formRef.current?.requestSubmit();
  };

  return (
    <div className="flex h-screen flex-col bg-background">
      {/* Header */}
      <header className="flex items-center gap-3 border-b px-4 py-3">
        <Link href="/">
          <Button variant="ghost" size="sm" className="gap-2">
            <ArrowLeft className="h-4 w-4" />
            Dashboard
          </Button>
        </Link>
        <div className="flex items-center gap-2">
          <Sparkles className="h-5 w-5 text-amber-500" />
          <h1 className="text-lg font-semibold">Ask about your data</h1>
        </div>
        <div className="ml-auto text-xs text-muted-foreground">
          Powered by Claude
        </div>
      </header>

      {/* Chat area */}
      <ChatContainerRoot className="relative flex-1 px-4">
        <ChatContainerContent className="mx-auto max-w-3xl gap-6 py-8">
          {messages.length === 0 ? (
            <EmptyState onSuggestionClick={handleSuggestionClick} />
          ) : (
            messages.map((message) => (
              <ChatMessage key={message.id} role={message.role} content={message.content} />
            ))
          )}

          {isLoading && messages[messages.length - 1]?.role === "user" && (
            <div className="flex gap-3">
              <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-amber-100 text-amber-700">
                <Sparkles className="h-4 w-4" />
              </div>
              <div className="rounded-lg bg-secondary p-3">
                <TextShimmerLoader text="Analyzing your data..." size="sm" />
              </div>
            </div>
          )}

          <ChatContainerScrollAnchor />
        </ChatContainerContent>

        {/* Scroll button — must be inside ChatContainerRoot for context */}
        <div className="pointer-events-none sticky bottom-4 flex justify-center">
          <div className="pointer-events-auto">
            <ScrollButton />
          </div>
        </div>
      </ChatContainerRoot>

      {/* Input area */}
      <div className="border-t bg-background px-4 py-4">
        <form
          ref={formRef}
          onSubmit={handleSubmit}
          className="mx-auto max-w-3xl"
        >
          <PromptInput
            value={input}
            onValueChange={setInput}
            isLoading={isLoading}
            onSubmit={handleKeySubmit}
            className="border-border"
          >
            <PromptInputTextarea placeholder="Ask anything about your studio data..." />
            <PromptInputActions className="justify-end px-2 pb-2">
              {isLoading ? (
                <PromptInputAction tooltip="Stop generating">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-9 w-9 rounded-full"
                    onClick={(e) => {
                      e.preventDefault();
                      stop();
                    }}
                  >
                    <Square className="h-4 w-4 fill-current" />
                  </Button>
                </PromptInputAction>
              ) : (
                <PromptInputAction tooltip="Send message">
                  <Button
                    type="submit"
                    variant="default"
                    size="sm"
                    className="h-9 w-9 rounded-full"
                    disabled={!input.trim()}
                  >
                    <Send className="h-4 w-4" />
                  </Button>
                </PromptInputAction>
              )}
            </PromptInputActions>
          </PromptInput>
        </form>
      </div>
    </div>
  );
}

function EmptyState({ onSuggestionClick }: { onSuggestionClick: (s: string) => void }) {
  return (
    <div className="flex flex-1 flex-col items-center justify-center gap-8 py-12">
      <div className="flex flex-col items-center gap-3 text-center">
        <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-amber-100 text-amber-700">
          <Sparkles className="h-7 w-7" />
        </div>
        <h2 className="text-xl font-semibold">Ask about Sky Ting</h2>
        <p className="max-w-md text-sm text-muted-foreground">
          I have access to your full dashboard data — revenue, subscribers, churn,
          merch, spa, conversions, and more. Ask me anything.
        </p>
      </div>
      <div className="flex max-w-2xl flex-wrap justify-center gap-2">
        {SUGGESTIONS.map((s) => (
          <PromptSuggestion
            key={s}
            className="text-xs"
            onClick={() => onSuggestionClick(s)}
          >
            {s}
          </PromptSuggestion>
        ))}
      </div>
    </div>
  );
}

function ChatMessage({ role, content }: { role: string; content: string }) {
  if (role === "user") {
    return (
      <Message className="justify-end">
        <MessageContent className="max-w-[80%] bg-primary text-primary-foreground">
          {content}
        </MessageContent>
      </Message>
    );
  }

  return (
    <Message>
      <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-amber-100 text-amber-700">
        <Sparkles className="h-4 w-4" />
      </div>
      <MessageContent
        markdown
        className="max-w-[80%] bg-transparent p-0 prose-headings:text-foreground prose-p:text-foreground prose-li:text-foreground prose-strong:text-foreground prose-td:text-foreground prose-th:text-foreground"
      >
        {content}
      </MessageContent>
    </Message>
  );
}
