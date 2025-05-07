---
hide:
  - toc
---

# Latest Publications

<div class="pub-entry">
    <div style="text-align: center;">
        <img src="/assets/publications/styx-pub.webp" class="pub-thumb" />
        <div>
        <a href="/assets/publications/styx.pdf" class="md-button ghost-button" target="_blank">
            📜 PDF
        </a>
        </div>
    </div>

  <div class="pub-meta">
    <div class="pub-venue">SIGMOD 2025</div>
    <div class="pub-title">Styx: Transactional Stateful Functions on Streaming Dataflows</div>
    <div class="pub-authors">
      Kyriakos Psarakis, George Christodoulou, George Siachamis, Marios Fragkoulis, Asterios Katsifodimos
    </div>
    <div class="pub-abstract">
    <p>
      Developing stateful cloud applications, such as low-latency workflows and microservices with strict consistency requirements,
      remains arduous for programmers. The Stateful Functions-as-a-Service (SFaaS) paradigm aims to serve these use cases. 
      However, existing approaches provide weak transactional guarantees or perform expensive external state accesses requiring 
      inefficient transactional protocols that increase execution latency.
    <br><br>
      In this paper, we present Styx, a novel dataflow-based SFaaS runtime that executes serializable transactions consisting of stateful functions
      that form arbitrary call-graphs with exactly-once guarantees. Styx extends a deterministic transactional protocol by contributing: 
      i) a function acknowledgment scheme to determine transaction boundaries required in SFaaS workloads,
      ii) a function-execution caching mechanism, 
     and iii) an early commit-reply mechanism that substantially reduces transaction execution latency. 
     Experiments with the YCSB, TPC-C, and Deathstar benchmarks show that Styx outperforms state-of-the-art approaches 
     by achieving at least one order of magnitude higher throughput while exhibiting near-linear scalability and low latency.
    </p>
    </div>
  </div>
</div>

<div class="pub-entry">
    <div style="text-align: center;">
        <img src="/assets/publications/styx_cidr-pub.webp" class="pub-thumb" />
        <div>
        <a href="/assets/publications/styx_cidr.pdf" class="md-button ghost-button" target="_blank">
            📜 PDF
        </a>
        </div>
    </div>
  <div class="pub-meta">
    <div class="pub-venue">CIDR 2025</div>
    <div class="pub-title">Transactional Cloud Applications Go with the (Data)Flow</div>
    <div class="pub-authors">
      Kyriakos Psarakis, George Christodoulou, Marios Fragkoulis, Asterios Katsifodimos
    </div>
    <div class="pub-abstract">
      <p>Traditional monolithic applications are migrated to the cloud, typically using a microservice-like architecture. 
Although this migration leads to significant benefits such as scalability and development agility, it also leaves behind 
the transactional guarantees that database systems have provided to monolithic applications for decades. In the cloud era, 
developers build transactional and fault-tolerant distributed applications by explicitly programming transaction protocols 
at the application level.<br><br>In this paper, we argue that the principles behind the streaming dataflow execution model 
and deterministic transactional protocols provide a powerful and suitable substrate for executing transactional cloud applications. 
To this end, we introduce Styx, a transactional application runtime based on streaming dataflows that enables an object-oriented programming 
model for scalable, fault-tolerant cloud applications with serializable guarantees.</p>
    </div>
  </div>
</div>

<div class="pub-entry">
    <div style="text-align: center;">
        <img src="/assets/publications/stateflow-pub.webp" class="pub-thumb" />
        <div>
        <a href="/assets/publications/stateflow.pdf" class="md-button ghost-button" target="_blank">
            📜 PDF
        </a>
        </div>
    </div>

  <div class="pub-meta">
    <div class="pub-venue">EDBT 2024</div>
    <div class="pub-title">Stateful Entities: Object-oriented Cloud Applications as Distributed Dataflows</div>
    <div class="pub-authors">
      Kyriakos Psarakis, Wouter Zorgdrager, Marios Fragkoulis, Guido Salvaneschi, Asterios Katsifodimos
    </div>
    <div class="pub-abstract">
      <p>Although the cloud has reached a state of robustness, the burden of using its resources falls on the shoulders of 
programmers who struggle to keep up with ever-growing cloud infrastructure services and abstractions. As a result, 
state management, scaling, operation, and failure management of scalable cloud applications, require disproportionately 
more effort than developing the applications&apos; actual business logic.<br><br>Our vision aims to raise the abstraction 
level for programming scalable cloud applications by compiling stateful entities -- &nbsp;a programming model enabling 
imperative transactional programs authored in Python -- &nbsp;into stateful streaming dataflows. We propose a compiler 
pipeline that analyzes the abstract syntax tree of stateful entities and transforms them into an intermediate representation 
based on stateful dataflow graphs. It then compiles that intermediate representation into different dataflow engines, leveraging 
their exactly-once message processing guarantees to prevent state or failure management primitives from "leaking"
into the level of the programming model. Preliminary experiments with a proof of concept implementation show that despite program 
transformation and translation to dataflows, stateful entities can perform at sub-100ms latency even for transactional workloads.</p>
    </div>
  </div>
</div>