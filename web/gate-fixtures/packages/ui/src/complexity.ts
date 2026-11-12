export function grade(n: number): string {
  if (n > 90) return "a";
  if (n > 80) return "b";
  if (n > 70) return "c";
  if (n > 60) return "d";
  if (n > 50) return "e";
  if (n > 40) return "f";
  if (n > 30) return "g";
  if (n > 20) return "h";
  if (n > 10) return "i";
  if (n > 0) return "j";
  return "k";
}
