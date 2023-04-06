import PrismCode from "../../../Components/Common/Prism";

// Slides Only

const slideOnlyCode =
    `
<!-- Slides Only -->
<UncontrolledCarousel
    controls={false}
    indicators={false}
    interval={3000}
    items={[
      {
        altText: " ",
        caption: " ",
        key: 1,
        src: img1,
      },
      {
        altText: " ",
        caption: " ",
        key: 2,
        src: img2,
      },
      {
        altText: " ",
        caption: " ",
        key: 3,
        src: img3,
      },
    ]}
  />
`;

const SlideOnlyExample = () => (
    <PrismCode
        code={slideOnlyCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// With Controls
const withControlCode =
    `
<!-- With Controls -->
<UncontrolledCarousel
    interval={4000}
    indicators={false}
    items={[
      {
        altText: " ",
        caption: " ",
        key: 1,
        src: img1,
      },
      {
        altText: " ",
        caption: " ",
        key: 2,
        src: img2,
      },
      {
        altText: " ",
        caption: " ",
        key: 3,
        src: img3,
      },
    ]}
  />
`;

const WithControlExample = () => (
    <PrismCode
        code={withControlCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// with Indicators
const withIndicatorCode =
    `
    <!-- With Indicators -->
<UncontrolledCarousel
  interval={4000}
    items={[
      {
        altText: " ",
        caption: " ",
        key: 1,
        src: img3,
      },
      {
        altText: " ",
        caption: " ",
        key: 2,
        src: img2,
      },
      {
        altText: " ",
        caption: " ",
        key: 3,
        src: img1,
      },
    ]}
  />
`;

const WithIndicatorExample = () => (
    <PrismCode
        code={withIndicatorCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// with Captions
const withCaptionCode =
    `
    <!-- With Captions -->
<UncontrolledCarousel
    interval={4000}
    items={[
      {
        altText: "First slide label",
        caption: "First slide label",
        key: 1,
        src: img7,
      },
      {
        altText: "Second slide label",
        caption: "Second slide label",
        key: 2,
        src: img2,
      },
      {
        altText: "Third slide label",
        caption: "Third slide label",
        key: 3,
        src: img9,
      },
    ]}
  />
`;

const WithCaptionExample = () => (
    <PrismCode
        code={withCaptionCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Crossfade Animation
const crossFadeCode =
    `
<!-- With Crossfade Animation -->
<UncontrolledCarousel
  interval={4000}
    items={[
      {
        altText: " ",
        caption: " ",
        key: 1,
        src: img1,
      },
      {
        altText: " ",
        caption: " ",
        key: 2,
        src: img2,
      },
      {
        altText: " ",
        caption: " ",
        key: 3,
        src: img3,
      },
    ]}
  />
`;

const CrossFadeExample = () => (
    <PrismCode
        code={crossFadeCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Individual carousel-item Interval
const induvidualIntervalCode =
    `
<!-- Individual Slide -->
<UncontrolledCarousel
    interval={4000}
    indicators={false}
    items={[
      {
        altText: " ",
        caption: " ",
        key: 1,
        src: img12,
      },
      {
        altText: " ",
        caption: " ",
        key: 2,
        src: img11,
      },
      {
        altText: " ",
        caption: " ",
        key: 3,
        src: img10,
      },
    ]}
  />
`;

const InduvidualIntervalExample = () => (
    <PrismCode
        code={induvidualIntervalCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Disable Touch Swiping
const disableTouchCode =
    `
<!-- Disable Touch Swiping -->
<UncontrolledCarousel
    interval={false}
    indicators={false}
    enableTouch={false}
    items={[
        {
            altText: " ",
            caption: " ",
            key: 1,
            src: img9,
        },
        {
            altText: " ",
            caption: " ",
            key: 2,
            src: img8,
        },
        {
            altText: " ",
            caption: " ",
            key: 3,
            src: img7,
        },
    ]}
/>
`;

const DisableTouchExample = () => (
    <PrismCode
        code={disableTouchCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

// Dark Variant
const darkVariantCode =
    `
    <!-- Dark Variant -->
<UncontrolledCarousel
    dark={true}
    interval={false}
    items={[
      {
        altText: " ",
        caption: "Drawing a sketch",
        key: 1,
        src: img1,
      },
      {
        altText: " ",
        caption: "Blue clock on a pastel background",
        key: 2,
        src: img2,
      },
      {
        altText: " ",
        caption: "Working at a coffee shop",
        key: 3,
        src: img3,
      },
    ]}
  />
`;

const DarkVariantExample = () => (
    <PrismCode
        code={darkVariantCode}
        language={("js", "css", "html")}
        plugins={["line-numbers"]}
    />
);

export { SlideOnlyExample, WithControlExample, WithIndicatorExample, WithCaptionExample, CrossFadeExample, InduvidualIntervalExample, DisableTouchExample, DarkVariantExample };