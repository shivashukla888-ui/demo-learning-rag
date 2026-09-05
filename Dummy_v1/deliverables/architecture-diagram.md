# Trade Surveillance Navigator — Solution Architecture

```mermaid
flowchart LR
    subgraph S[Enterprise and Market Data]
      T[Trades and Orders]
      P[Positions and Exposure]
      M[Market Data]
      C[Accounts, Clients and Traders]
      O[Ownership and Reference Data]
    end

    subgraph J[Java 21 / Spring Boot Control Plane]
      API[Surveillance REST API]
      N[Validation and Normalisation]
      W[Case and Human-Review Workflow]
      A[Audit and Version Records]
    end

    subgraph PY[Python / FastAPI Intelligence Plane]
      D[Deterministic Feature Engine]
      ML[EBM Ranking and Isolation Forest]
      CL[Constrained Alert Clustering]
      ER[Probabilistic Entity Resolution]
      G[Graph and Coordination Analytics]
    end

    subgraph E[Evidence and Copilot Plane]
      ES[(Immutable Evidence Store)]
      L[Claim → Metric → Evidence Lineage]
      LLM[Retrieval-Constrained LLM]
    end

    subgraph U[Investigator Experience]
      Q[Prioritised Case Queue]
      UI[React Investigation Workbench]
      H[Human Decision]
    end

    T --> N
    P --> N
    M --> N
    C --> N
    O --> N
    N --> API
    API --> D
    D --> ML
    D --> CL
    C --> ER
    O --> ER
    ER --> G
    D --> G
    ML --> API
    CL --> API
    G --> API
    D --> ES
    ER --> ES
    API --> ES
    ES --> L
    L --> LLM
    LLM --> API
    API --> Q
    Q --> UI
    UI --> H
    H --> W
    W --> A
    A --> UI

    classDef java fill:#dbeafe,stroke:#2563eb,color:#10233f;
    classDef python fill:#dcfce7,stroke:#16815f,color:#102b23;
    classDef evidence fill:#fef3c7,stroke:#ca8a04,color:#3d2f08;
    classDef human fill:#fee2e2,stroke:#dc4c4c,color:#3b1212;
    class API,N,W,A java;
    class D,ML,CL,ER,G python;
    class ES,L,LLM evidence;
    class H human;
```

## Technology boundary

- Java owns APIs, validation, orchestration, workflow, decisions and audit records.
- Python owns deterministic surveillance features, statistical/ML scoring, clustering, entity resolution and graph analytics.
- The LLM receives verified evidence only and generates cited summaries; it does not calculate risk or decide whether abuse occurred.
- The investigator remains the only authority for escalation, further review, false-positive disposition or closure.
