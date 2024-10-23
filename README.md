# Queezer

## Overview

Queezer is an open-source Python package designed to help you start owning your own open-source LLM models by collecting request and response data from API calls. With just one line of code, Queezer integrates seamlessly into your existing workflows, making it easy to gather crucial data for building your models, rather than relying solely on external API calls.

The package is lightweight, requires only the `requests` library, and can be integrated in under 5 minutes. It is ideal for startups and SMEs looking to take ownership of their AI models and data, ensuring that their unique intellectual property remains theirs.

## Features

- **Easy Integration:** One import, one line of code to start collecting data.
- **Lightweight:** Minimal dependencies, requires only the `requests` library.
- **Model Ownership:** Helps you gather and own the data needed to build and train your AI models.

## Getting Started

To install Queezer, simply run:

```bash
pip install queezer
```

## Usage

Here's a basic example to get you started:

```Python
import openai
from queezer import Queezer

client = openai.OpenAI(api_key="your-openAI-api-key")
queezer = Queezer(api_key="your-queezer-api-key")
# queezer = Queezer()
# ^ In case you do not have an api key
# this instantiates a local SQLite Database

# Your code where you make API calls
# previously: result = client.chat.completions.create(...)
result = queezer.squeeze(client.chat.completions.create,
        tags=["test", "gpt-4o-mini"],
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {
                "role": "user",
                "content": "Write a short poem about recursion in programming."
            }
        ]
    )

```

That's it! With one line of code, Queezer begins collecting the API request and response data for you.

## Roadmap

- [x] **Streaming:** We plan to support streaming requests in the near future.
- [ ] **Multi Modality:** Expansion to handle more complex data inputs and outputs (e.g., images, video, text) is on the way.
- [ ] **Typing Support**
- [ ] **Thread Offloading**
- [ ] **Async Support**

## Contributing

We welcome contributions from the community! To contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -m 'Add a new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Open a pull request, and we'll review it as soon as possible.

Feel free to report issues or suggest features by opening an issue in the repository.

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

---

We hope Queezer empowers your startup to take control of your data and models. Happy coding!
