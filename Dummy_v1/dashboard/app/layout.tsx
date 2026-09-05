import type { Metadata } from 'next';
import './globals.css';
import './modules.css';
import './robust.css';

export const metadata: Metadata = {
  metadataBase: new URL('https://aegis-trade-surveillance-copilot.shivashukla-888.chatgpt.site'),
  title: 'Trade Surveillance Navigator',
  description: 'Explainable, evidence-backed, human-controlled trade surveillance across multiple market-abuse typologies.',
  openGraph: {
    title: 'Trade Surveillance Navigator',
    description: 'AI prioritises. Evidence explains. Human decides.',
    images: ['/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Trade Surveillance Navigator',
    description: 'AI prioritises. Evidence explains. Human decides.',
    images: ['/og.png'],
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
