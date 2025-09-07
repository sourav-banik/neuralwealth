import gradio as gr
from typing import List
from neuralwealth.ui.api.rest import generate_chat_response
from neuralwealth.ui.ui_orchestrator import ui_orchestrator

class KGRAGChat:
    """Gradio-based chat interface for KG-RAG queries"""
    
    def __init__(self):
        self.chat_history = []
        self.session_id = None
        
    def initialize_session(self):
        """Initialize or retrieve session"""
        if not self.session_id:
            self.session_id = ui_orchestrator.create_session()
        return self.session_id
    
    def chat_interface(self, message: str, history: List[dict]) -> List[dict]:
        """Main chat interface function"""
        session_id = self.initialize_session()
        
        # Generate response using the REST API logic
        response = generate_chat_response(message)
        
        # Add to orchestrator's chat history
        ui_orchestrator.add_chat_message(session_id, message, response)
        
        # Update gradio history with correct format
        history.append({"role": "user", "content": message})
        history.append({"role": "assistant", "content": response})
        return history
    
    def get_chat_examples(self) -> List[List[str]]:
        """Get example queries for users"""
        return [
            ["Why did you buy AAPL?"],
            ["What affects technology stocks?"],
            ["How would my portfolio perform in a 2008 crash?"],
            ["Explain my current allocation"],
            ["What strategies are available?"]
        ]
    
    def create_interface(self):
        """Create Gradio chat interface"""
        with gr.Blocks(title="NeuralWealth KG-RAG Chat", theme="soft") as demo:
            gr.Markdown("# 🤖 NeuralWealth Knowledge Chat")
            gr.Markdown("Ask questions about your portfolio, strategies, and market scenarios.")
            
            with gr.Row():
                with gr.Column(scale=2):
                    chatbot = gr.Chatbot(
                        value=[],
                        type='messages',
                        label="Chat History",
                        height=500
                    )
                    
                    with gr.Row():
                        msg = gr.Textbox(
                            label="Your Question",
                            placeholder="Ask about your portfolio, strategies, or scenarios...",
                            lines=1
                        )
                        submit = gr.Button("Send", variant="primary")
                
                with gr.Column(scale=1):
                    gr.Markdown("### Example Questions")
                    examples = gr.Examples(
                        examples=self.get_chat_examples(),
                        inputs=msg,
                        label="Try these questions:"
                    )
                    
                    gr.Markdown("### Capabilities")
                    gr.Markdown("""
                    - 📊 Portfolio explanation
                    - 📈 Strategy analysis  
                    - 🎯 Causal relationships
                    - ⚡ Scenario simulation
                    - 🔍 Risk assessment
                    """)
            
            # Event handlers
            submit.click(
                self.chat_interface,
                inputs=[msg, chatbot],
                outputs=[chatbot]
            )
            msg.submit(
                self.chat_interface,
                inputs=[msg, chatbot],
                outputs=[chatbot]
            )
            
        return demo

# Global chat instance
kg_rag_chat = KGRAGChat()

def launch_chat():
    """Launch the chat interface"""
    demo = kg_rag_chat.create_interface()
    demo.launch(server_name="0.0.0.0", server_port=7860, share=False)

if __name__ == "__main__":
    launch_chat()